package tpf;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Hub Publish/Subscribe sobre WebSocket Alojado no Processo do Líder (LeaderBot).
 * 
 * PAPEL NO SISTEMA:
 * Constitui a infraestrutura central de rede que interliga o Líder e todas as Réplicas Voluntárias:
 * 1. Autenticação Criptográfica Forte (Ed25519):
 *    - No 1.º registo, exige um token efémero (válido por 10 min) gerado no Discord via /replica-token
 *      e uma chave pública Ed25519;
 *    - Em todas as ligações, envia um DESAFIO com nonce aleatório (32 bytes) e valida a PROVA (assinatura Ed25519);
 *    - O token só é consumido e a réplica só é registada em 'replicas.txt' após verificação bem-sucedida da PROVA.
 * 2. Máquina de Estados por Ligação:
 *    - NOVA -> DESAFIADA -> AUTENTICADA. Qualquer mensagem fora de ordem ou ilegal desliga o cliente imediatamente.
 * 3. Controlo Rigoroso de Acesso a Tópicos (Isolamento Zero-Knowledge):
 *    - Uma réplica apenas pode subscrever o seu tópico privado 'tpf-r-<o seu id>' e o de controlo 'tpf-ctl';
 *    - Uma réplica apenas pode publicar no tópico 'tpf-lider';
 *    - Qualquer tentativa de subscrever ou espiar tópicos de terceiros fecha a conexão e regista 'ERRO'.
 * 4. Encaminhamento em Memória:
 *    - publish(topico, msg) envia mensagens apenas aos WebSockets subscritos no respetivo tópico;
 *    - Mensagens publicadas em 'tpf-lider' são entregues diretamente em processo ao LeaderBot
 *      com o identificador autenticado da sessão WebSocket garantido pela camada de transporte.
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - Estende org.java_websocket.server.WebSocketServer, que gere o seu próprio grupo interno de threads:
 *   * 1 Thread Select / Accept de I/O de rede;
 *   * Múltiplas Threads Worker de Despacho (Worker Threads) que invocam onOpen, onMessage, onClose e onError.
 * - Estruturas de dados concorrentes (ConcurrentHashMap) para sessoes, tokens, chavesRegistadas e subscricoes.
 * - Métodos de gravação de ficheiros (replicas.txt) protegidos com 'synchronized(this)' e escritos com ATOMIC_MOVE.
 * 
 * ONDE É UTILIZADA:
 * Instanciada e iniciada no arranque do LeaderBot (hub.start()).
 */
public final class PubSubHub extends WebSocketServer {

    /**
     * Estados do handshake de autenticação de cada socket conectado.
     */
    private enum EstadoLigacao {
        NOVA, DESAFIADA, AUTENTICADA
    }

    /**
     * Contexto de sessão mantido para cada cliente WebSocket conectado.
     */
    private static final class Sessao {
        EstadoLigacao estado = EstadoLigacao.NOVA;
        String replicaId;
        String nonceB64;
        PublicKey chavePendente;
        String tokenPendente;
        final Set<String> topicos = ConcurrentHashMap.newKeySet();
    }

    /**
     * Informação de controlo de tokens efémeros de registo.
     */
    private static final class TokenInfo {
        final long criadoMs;
        boolean usado = false;

        TokenInfo(long criadoMs) {
            this.criadoMs = criadoMs;
        }
    }

    private final Path replicasPath;
    private final Map<String, PublicKey> chavesRegistadas = new ConcurrentHashMap<>();
    private final Map<String, String> chavesB64 = new ConcurrentHashMap<>();
    private final Map<String, TokenInfo> tokens = new ConcurrentHashMap<>();
    private final Map<WebSocket, Sessao> sessoes = new ConcurrentHashMap<>();
    private final Map<String, Set<WebSocket>> subscricoes = new ConcurrentHashMap<>();

    private final BiConsumer<String, String> onMensagemLider; // Callback: (replicaIdAutenticado, payload)
    private final BiConsumer<String, String> onReplicaAutenticada; // Callback: (replicaId, evento)
    private final BiConsumer<String, String> eventLogger;
    private final SecureRandom rng = new SecureRandom();

    /**
     * Cria e configura o hub Pub/Sub.
     *
     * @param porto Porto de escuta TCP (normalmente Wire.PORTO = 8765)
     * @param baseDir Diretório de trabalho onde reside 'replicas.txt'
     * @param onMensagemLider Callback invocada quando uma réplica publica em 'tpf-lider'
     * @param onReplicaAutenticada Callback invocada quando uma réplica conclui com sucesso a autenticação
     * @param eventLogger Callback para gravação de eventos operacionais em 'eventos-lider.txt'
     */
    public PubSubHub(int porto, Path baseDir,
                     BiConsumer<String, String> onMensagemLider,
                     BiConsumer<String, String> onReplicaAutenticada,
                     BiConsumer<String, String> eventLogger) {
        super(new InetSocketAddress(porto));
        setReuseAddr(true);
        this.replicasPath = baseDir.resolve("replicas.txt");
        this.onMensagemLider = onMensagemLider;
        this.onReplicaAutenticada = onReplicaAutenticada;
        this.eventLogger = eventLogger;
        carregarReplicas();
    }

    /**
     * Lê as réplicas já autorizadas e as suas chaves públicas Ed25519 de 'replicas.txt' no arranque.
     */
    private synchronized void carregarReplicas() {
        if (!Files.exists(replicasPath)) return;
        try {
            for (String line : Files.readAllLines(replicasPath, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                String[] parts = line.split(";", 2);
                if (parts.length == 2) {
                    String id = parts[0].trim();
                    String pkB64 = parts[1].trim();
                    PublicKey pk = parseEd25519PublicKey(pkB64);
                    chavesRegistadas.put(id, pk);
                    chavesB64.put(id, pkB64);
                }
            }
            System.out.println("Hub carregou " + chavesRegistadas.size() + " réplicas de replicas.txt.");
        } catch (Exception e) {
            System.err.println("Erro ao ler replicas.txt: " + e.getMessage());
        }
    }

    /**
     * Persiste atomicamente a lista autorizada de réplicas em 'replicas.txt' (Temporary File + ATOMIC_MOVE).
     */
    private synchronized void gravarReplicasAtomico() {
        try {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : chavesB64.entrySet()) {
                sb.append(e.getKey()).append(";").append(e.getValue()).append("\n");
            }
            Path tmp = replicasPath.resolveSibling("replicas.txt.tmp");
            Files.write(tmp, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, replicasPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Erro ao gravar replicas.txt: " + e.getMessage());
        }
    }

    /**
     * Gera um novo token efémero aleatório de 18 bytes codificado em URL-safe Base64.
     * Válido por 10 minutos para registo único de uma réplica.
     * 
     * Onde é utilizado: LeaderBot ao responder ao slash command /replica-token.
     */
    public String gerarToken() {
        byte[] buf = new byte[18];
        rng.nextBytes(buf);
        String tok = Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
        tokens.put(tok, new TokenInfo(System.currentTimeMillis()));
        return tok;
    }

    /**
     * Revoga a autorização de uma réplica: remove a chave pública de 'replicas.txt',
     * persiste o ficheiro e fecha imediatamente qualquer ligação WebSocket ativa dessa réplica.
     * 
     * Onde é utilizado: LeaderBot ao responder ao slash command /replica-remover <id>.
     */
    public synchronized boolean revogarReplica(String id) {
        PublicKey removida = chavesRegistadas.remove(id);
        chavesB64.remove(id);
        if (removida != null) {
            gravarReplicasAtomico();
        }
        for (Map.Entry<WebSocket, Sessao> entry : sessoes.entrySet()) {
            if (id.equals(entry.getValue().replicaId)) {
                entry.getKey().close(1008, "Replica removida pelo administrador");
            }
        }
        return removida != null;
    }

    /**
     * Publica uma mensagem num tópico, entregando-a a todas as conexões que o subscreveram.
     * 
     * Onde é utilizado: LeaderBot.distribuirChunkReplicas() para enviar 'STORE' em 'tpf-r-<id>'.
     */
    public void publish(String topico, String mensagem) {
        Set<WebSocket> subs = subscricoes.get(topico);
        if (subs == null) return;
        for (WebSocket ws : subs) {
            if (ws.isOpen()) {
                ws.send(mensagem);
            }
        }
    }

    @Override
    public void onStart() {
        System.out.println("PubSubHub WebSocket ativo no porto " + getPort());
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        sessoes.put(conn, new Sessao());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        Sessao s = sessoes.remove(conn);
        if (s != null) {
            for (String topico : s.topicos) {
                Set<WebSocket> set = subscricoes.get(topico);
                if (set != null) set.remove(conn);
            }
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("Erro WebSocket no Hub: " + ex.getMessage());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        Sessao s = sessoes.get(conn);
        if (s == null) {
            conn.close();
            return;
        }

        try {
            if (s.estado == EstadoLigacao.NOVA) {
                processarOla(conn, s, message);
            } else if (s.estado == EstadoLigacao.DESAFIADA) {
                processarProva(conn, s, message);
            } else if (s.estado == EstadoLigacao.AUTENTICADA) {
                processarAutenticada(conn, s, message);
            }
        } catch (Exception e) {
            rejeitarEDesligar(conn, s, "Excecao no protocolo: " + e.getMessage());
        }
    }

    /**
     * Processa a mensagem inicial OLA:
     * - 1.º registo: OLA;<id>;<chavePublicaB64>;<token>
     * - Ligações seguintes: OLA;<id>
     * Gera um nonce aleatório de 32 bytes e responde com 'DESAFIO;<nonceB64>'.
     */
    private void processarOla(WebSocket conn, Sessao s, String msg) throws Exception {
        String[] parts = msg.split(";", 4);
        if (!"OLA".equals(parts[0]) || parts.length < 2) {
            rejeitarEDesligar(conn, s, "Primeira mensagem invalida (esperado OLA)");
            return;
        }

        String id = parts[1].trim();
        if (id.isEmpty()) {
            rejeitarEDesligar(conn, s, "ID de replica vazio");
            return;
        }

        if (parts.length == 4) {
            // Primeiro registo com token
            String pkB64 = parts[2].trim();
            String token = parts[3].trim();

            if (chavesRegistadas.containsKey(id)) {
                rejeitarEDesligar(conn, s, "Replica ja registada; ligacoes seguintes devem usar OLA;<id>: " + id);
                return;
            }

            TokenInfo ti = tokens.get(token);
            if (ti == null || ti.usado || (System.currentTimeMillis() - ti.criadoMs) > Wire.TOKEN_VALIDADE_MS) {
                rejeitarEDesligar(conn, s, "Token invalido, expirado ou ja utilizado para " + id);
                return;
            }

            s.chavePendente = parseEd25519PublicKey(pkB64);
            s.tokenPendente = token;
        } else if (parts.length == 2) {
            // Religações subsequentes da réplica
            PublicKey pk = chavesRegistadas.get(id);
            if (pk == null) {
                rejeitarEDesligar(conn, s, "Replica nao registada (falta token): " + id);
                return;
            }
            s.chavePendente = pk;
        } else {
            rejeitarEDesligar(conn, s, "Formato OLA invalido");
            return;
        }

        // Gera desafio criptográfico novo de uso único (32 bytes)
        byte[] nonce = new byte[32];
        rng.nextBytes(nonce);
        s.replicaId = id;
        s.nonceB64 = Base64.getEncoder().encodeToString(nonce);
        s.estado = EstadoLigacao.DESAFIADA;

        conn.send("DESAFIO;" + s.nonceB64);
    }

    /**
     * Valida a resposta 'PROVA;<id>;<assinaturaB64>' enviada pela réplica.
     * Consome o token e persiste o par em 'replicas.txt' apenas se a assinatura for válida.
     */
    private void processarProva(WebSocket conn, Sessao s, String msg) throws Exception {
        String[] parts = msg.split(";", 3);
        if (parts.length != 3 || !"PROVA".equals(parts[0])) {
            rejeitarEDesligar(conn, s, "Mensagem fora de ordem (esperado PROVA)");
            return;
        }

        String id = parts[1].trim();
        String sigB64 = parts[2].trim();

        if (!s.replicaId.equals(id)) {
            rejeitarEDesligar(conn, s, "ID na PROVA difere do OLA");
            return;
        }

        if (!verificarProva(id, s.nonceB64, sigB64, s.chavePendente)) {
            rejeitarEDesligar(conn, s, "Assinatura Ed25519 invalida na PROVA de " + id);
            return;
        }

        // Consome o token de uso único e persiste a réplica em replicas.txt
        if (s.tokenPendente != null) {
            synchronized (this) {
                TokenInfo ti = tokens.get(s.tokenPendente);
                if (ti == null || ti.usado || (System.currentTimeMillis() - ti.criadoMs) > Wire.TOKEN_VALIDADE_MS) {
                    rejeitarEDesligar(conn, s, "Token expirou ou foi consumido concorrentemente");
                    return;
                }
                ti.usado = true;
                String pkB64 = Base64.getEncoder().encodeToString(s.chavePendente.getEncoded());
                chavesRegistadas.put(id, s.chavePendente);
                chavesB64.put(id, pkB64);
                gravarReplicasAtomico();
            }
        }

        s.nonceB64 = null; // Descarta o nonce após validação
        s.estado = EstadoLigacao.AUTENTICADA;
        onReplicaAutenticada.accept(id, "AUTENTICADA");
    }

    /**
     * Processa comandos de subscrição (SUB) e publicação (PUB) após o nó estar AUTENTICADO.
     * Aplica as regras estritas de isolamento de tópicos.
     */
    private void processarAutenticada(WebSocket conn, Sessao s, String msg) {
        if (msg.startsWith("SUB;")) {
            String topico = msg.substring(4).trim();
            String permitido = Wire.topicReplica(s.replicaId);
            // Só pode subscrever o seu próprio tópico tpf-r-<id> ou o tópico global de controlo tpf-ctl
            if (!topico.equals(permitido) && !topico.equals(Wire.TOPICO_CTL)) {
                rejeitarEDesligar(conn, s, "Subscricao proibida de " + s.replicaId + " no topico " + topico);
                return;
            }
            s.topicos.add(topico);
            subscricoes.computeIfAbsent(topico, k -> ConcurrentHashMap.newKeySet()).add(conn);
        } else if (msg.startsWith("PUB;")) {
            String[] parts = msg.split(";", 3);
            if (parts.length < 3) {
                rejeitarEDesligar(conn, s, "PUB mal formatado");
                return;
            }
            String topico = parts[1].trim();
            String payload = parts[2];

            // Réplicas só podem publicar para o tópico do líder
            if (!Wire.TOPICO_LIDER.equals(topico)) {
                rejeitarEDesligar(conn, s, "Publicacao proibida de " + s.replicaId + " no topico " + topico);
                return;
            }
            // Entrega a mensagem ao LeaderBot acompanhada do ID autenticado na sessão
            onMensagemLider.accept(s.replicaId, payload);
        } else {
            rejeitarEDesligar(conn, s, "Comando invalido apos autenticacao: " + msg);
        }
    }

    /**
     * Valida a assinatura digital Ed25519 do desafio.
     * Dados assinados: "tpf-registo|" + id + "|" + nonceB64 (em UTF-8).
     * Vincula a assinatura tanto ao nonce como ao identificador da ligação, prevenindo ataques de personificação.
     */
    public static boolean verificarProva(String id, String nonceB64, String sigB64, PublicKey pubKey) {
        try {
            byte[] dados = ("tpf-registo|" + id + "|" + nonceB64).getBytes(StandardCharsets.UTF_8);
            byte[] sig = Base64.getDecoder().decode(sigB64);
            Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(pubKey);
            verifier.update(dados);
            return verifier.verify(sig);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Converte uma chave pública codificada em Base64 (formato X.509) num objeto PublicKey Ed25519.
     */
    private static PublicKey parseEd25519PublicKey(String pkB64) throws Exception {
        byte[] bytes = Base64.getDecoder().decode(pkB64);
        KeyFactory kf = KeyFactory.getInstance("Ed25519");
        return kf.generatePublic(new X509EncodedKeySpec(bytes));
    }

    /**
     * Regista um evento de erro no log e encerra a conexão WebSocket com código de fecho 1008 (Policy Violation).
     */
    private void rejeitarEDesligar(WebSocket conn, Sessao s, String motivo) {
        String quem = (s != null && s.replicaId != null) ? s.replicaId : "desconhecido";
        System.err.println("[HUB ERRO] (" + quem + "): " + motivo);
        eventLogger.accept("ERRO", quem + " - " + motivo);
        conn.close(1008, motivo);
    }
}
