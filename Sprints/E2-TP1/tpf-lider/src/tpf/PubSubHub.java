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
 * Hub Publish/Subscribe sobre WebSocket alojado no processo do Líder.
 * Gere autenticação desafio-resposta Ed25519, tokens de 1.º registo e encaminhamento de tópicos.
 */
public final class PubSubHub extends WebSocketServer {

    private enum EstadoLigacao {
        NOVA, DESAFIADA, AUTENTICADA
    }

    private static final class Sessao {
        EstadoLigacao estado = EstadoLigacao.NOVA;
        String replicaId;
        String nonceB64;
        PublicKey chavePendente;
        String tokenPendente;
        final Set<String> topicos = ConcurrentHashMap.newKeySet();
    }

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

    private final BiConsumer<String, String> onMensagemLider; // (replicaIdAutenticado, payload)
    private final BiConsumer<String, String> onReplicaAutenticada; // (replicaId, evento)
    private final BiConsumer<String, String> eventLogger;
    private final SecureRandom rng = new SecureRandom();

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

    public String gerarToken() {
        byte[] buf = new byte[18];
        rng.nextBytes(buf);
        String tok = Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
        tokens.put(tok, new TokenInfo(System.currentTimeMillis()));
        return tok;
    }

    public synchronized boolean revogarReplica(String id) {
        PublicKey removida = chavesRegistadas.remove(id);
        chavesB64.remove(id);
        if (removida != null) {
            gravarReplicasAtomico();
        }
        // Desliga qualquer sessão ativa dessa réplica
        for (Map.Entry<WebSocket, Sessao> entry : sessoes.entrySet()) {
            if (id.equals(entry.getValue().replicaId)) {
                entry.getKey().close(1008, "Replica removida pelo administrador");
            }
        }
        return removida != null;
    }

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
            // 1.º registo: OLA;<id>;<chavePublicaB64>;<token>
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
            // Ligações seguintes: OLA;<id>
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

        byte[] nonce = new byte[32];
        rng.nextBytes(nonce);
        s.replicaId = id;
        s.nonceB64 = Base64.getEncoder().encodeToString(nonce);
        s.estado = EstadoLigacao.DESAFIADA;

        conn.send("DESAFIO;" + s.nonceB64);
    }

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

        // Consome o token e persiste a chave apenas após a PROVA válida
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

        s.nonceB64 = null; // Descarta o nonce após uso
        s.estado = EstadoLigacao.AUTENTICADA;
        onReplicaAutenticada.accept(id, "AUTENTICADA");
    }

    private void processarAutenticada(WebSocket conn, Sessao s, String msg) {
        if (msg.startsWith("SUB;")) {
            String topico = msg.substring(4).trim();
            String permitido = Wire.topicReplica(s.replicaId);
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

            if (!Wire.TOPICO_LIDER.equals(topico)) {
                rejeitarEDesligar(conn, s, "Publicacao proibida de " + s.replicaId + " no topico " + topico);
                return;
            }
            onMensagemLider.accept(s.replicaId, payload);
        } else {
            rejeitarEDesligar(conn, s, "Comando invalido apos autenticacao: " + msg);
        }
    }

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

    private static PublicKey parseEd25519PublicKey(String pkB64) throws Exception {
        byte[] bytes = Base64.getDecoder().decode(pkB64);
        KeyFactory kf = KeyFactory.getInstance("Ed25519");
        return kf.generatePublic(new X509EncodedKeySpec(bytes));
    }

    private void rejeitarEDesligar(WebSocket conn, Sessao s, String motivo) {
        String quem = (s != null && s.replicaId != null) ? s.replicaId : "desconhecido";
        System.err.println("[HUB ERRO] (" + quem + "): " + motivo);
        eventLogger.accept("ERRO", quem + " - " + motivo);
        conn.close(1008, motivo);
    }
}