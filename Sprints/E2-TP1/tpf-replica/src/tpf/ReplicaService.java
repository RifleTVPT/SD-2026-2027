package tpf;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Serviço de Réplica Voluntária do Arquivo Distribuído (tpf-replica).
 * 
 * PAPEL NO SISTEMA:
 * Representa um nó de armazenamento independente executado numa máquina voluntária de terceiros:
 * - Não confia no Líder nem o Líder confia nela (Modelo Zero-Knowledge):
 *   * Guarda apenas bytes cifrados (.bin) e metadados opacos (.hdr);
 *   * Nunca tem acesso à master key, ao catálogo nem ao conteúdo em claro dos registos;
 * - Respeita a sua quota física de armazenamento local em disco (quotaBytes);
 * - Gere a sua própria identidade criptográfica persistente com um par de chaves Ed25519
 *   gerado na pasta 'dados-<id>/' (chave.priv e chave.pub);
 * - Autentica-se perante o Líder através de desafio-resposta Ed25519;
 * - Emite Heartbeats regulares (a cada 2 s) reportando espaço ocupado e quota;
 * - Ao receber um pedido 'STORE', verifica a quota, valida o hash SHA-256 do ficheiro cifrado,
 *   persiste atomicamente em disco (.bin e .hdr) e apenas depois responde com 'ACK'.
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * 1. Thread Main: Lê argumentos da linha de comandos, inicializa chaves e arranca o serviço;
 * 2. Threads do Cliente WebSocket do JDK (HttpClient): Recebem eventos de rede de forma assíncrona
 *    (onOpen, onText, onClose, onError) e invocam o processamento de STORE e DESAFIO;
 * 3. Thread do Temporizador de Heartbeat (ScheduledExecutorService "replica-hb"):
 *    Dispara a cada PERIODO_HB_MS (2 s) publicando 'PUB;tpf-lider;HB;...';
 * 4. Mecanismo de Backoff Exponencial: Agenda tentativas de religação com espera duplicada
 *    (de 1 s até um teto de 30 s) caso o Líder esteja temporariamente indisponível.
 * 
 * HIERARQUIA DE SINCRONIZAÇÃO:
 * - A atualização e verificação de bytesUsados e a escrita de ficheiros são sincronizadas
 *   (synchronized) para impedir que múltiplos STORE simultâneos estourem a quota local.
 * - AtomicBoolean protege o agendamento único de religações.
 * 
 * COMO EXECUTAR:
 * java -cp out tpf.ReplicaService <id> <url> <quotaBytes> [token]
 * Exemplo:
 * java -cp out tpf.ReplicaService A ws://localhost:8765 8388608 TOKEN_DISCORD
 */
public final class ReplicaService implements WebSocket.Listener {

    private final String id;
    private final URI uri;
    private final long quotaBytes;
    private String tokenRegisto;

    private final Path pastaDados;
    private final Path ficheiroEventos;
    private KeyPair keyPair;
    private long bytesUsados = 0;

    private volatile WebSocket wsAtual;
    private final StringBuilder bufferRececao = new StringBuilder();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "replica-hb");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> hbTask;
    private long backoffMs = Wire.BACKOFF_MIN_MS;
    private final AtomicBoolean religacaoAgendada = new AtomicBoolean();

    // Flag opcional para a Experiência 8 do guião (simular hash errado no ACK)
    private static final boolean CORROMPER_ACK = Boolean.getBoolean("tpf.corromperAck");
    // Flag opcional para a Experiência 9 do guião (simular ACK com ID forjado)
    private static final String FORJAR_ID = System.getProperty("tpf.forjarId", "");

    /**
     * Inicializa o serviço da réplica, criando a pasta de dados e carregando as chaves Ed25519.
     */
    public ReplicaService(String id, String url, long quotaBytes, String token) throws Exception {
        this.id = id;
        this.uri = URI.create(url);
        this.quotaBytes = quotaBytes;
        this.tokenRegisto = token;

        this.pastaDados = Paths.get("dados-" + id);
        this.ficheiroEventos = Paths.get("eventos-" + id + ".txt");
        Files.createDirectories(pastaDados);

        inicializarChavesEd25519();
        calcularBytesUsados();
    }

    /**
     * Carrega as chaves Ed25519 existentes em dados-<id>/ ou gera um novo par se for a 1.ª execução.
     * Assegura que o ficheiro da chave privada tem permissões restritas apenas ao dono.
     */
    private void inicializarChavesEd25519() throws Exception {
        Path privPath = pastaDados.resolve("chave.priv");
        Path pubPath = pastaDados.resolve("chave.pub");

        if (Files.exists(privPath) && Files.exists(pubPath)) {
            byte[] privBytes = Files.readAllBytes(privPath);
            byte[] pubBytes = Files.readAllBytes(pubPath);
            KeyFactory kf = KeyFactory.getInstance("Ed25519");
            PrivateKey priv = kf.generatePrivate(new PKCS8EncodedKeySpec(privBytes));
            PublicKey pub = kf.generatePublic(new X509EncodedKeySpec(pubBytes));
            this.keyPair = new KeyPair(pub, priv);
            System.out.println("[" + id + "] Par de chaves Ed25519 carregado de " + pastaDados);
        } else {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("Ed25519");
            this.keyPair = kpg.generateKeyPair();
            Files.write(privPath, keyPair.getPrivate().getEncoded(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.write(pubPath, keyPair.getPublic().getEncoded(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            restringirApenasDono(privPath);
            System.out.println("[" + id + "] Novo par de chaves Ed25519 gerado em " + pastaDados);
        }
    }

    /**
     * Restringe as permissões do sistema de ficheiros para que apenas o dono leia a chave privada.
     */
    private void restringirApenasDono(Path path) {
        try {
            if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
            } else {
                AclFileAttributeView aclView = Files.getFileAttributeView(path, AclFileAttributeView.class);
                if (aclView != null) {
                    UserPrincipal owner = aclView.getOwner();
                    AclEntry entry = AclEntry.newBuilder()
                            .setType(AclEntryType.ALLOW)
                            .setPrincipal(owner)
                            .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                            .build();
                    aclView.setAcl(Collections.singletonList(entry));
                }
            }
        } catch (Exception ignored) {}
    }

    /**
     * Calcula o total de bytes ocupados somando o tamanho de todos os ficheiros .bin existentes na pasta de dados.
     */
    private synchronized void calcularBytesUsados() throws IOException {
        long soma = 0;
        try (Stream<Path> files = Files.list(pastaDados)) {
            for (Path p : files.toList()) {
                if (p.getFileName().toString().endsWith(".bin")) {
                    soma += Files.size(p);
                }
            }
        }
        this.bytesUsados = soma;
    }

    /**
     * Regista um evento operacional no log local 'eventos-<id>.txt' (timestamp;evento;detalhe).
     */
    private synchronized void logEvento(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.write(ficheiroEventos, linha.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Erro ao gravar evento da réplica: " + e.getMessage());
        }
    }

    /**
     * Inicia a primeira ligação ao Hub.
     */
    public void iniciar() {
        conectar();
    }

    /**
     * Estabelece a ligação WebSocket assíncrona ao Líder usando o HttpClient do JDK.
     */
    private void conectar() {
        religacaoAgendada.set(false);
        System.out.println("[" + id + "] A ligar a " + uri + "...");
        HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(uri, this)
                .whenComplete((ws, err) -> {
                    if (err != null) {
                        System.err.println("[" + id + "] Falha na ligação: " + err.getMessage());
                        agendarReligacao();
                    }
                });
    }

    /**
     * Agenda uma nova tentativa de ligação com backoff exponencial (1 s, 2 s, 4 s... até 30 s).
     */
    private void agendarReligacao() {
        if (!religacaoAgendada.compareAndSet(false, true)) return;
        if (hbTask != null) {
            hbTask.cancel(false);
            hbTask = null;
        }
        long espera = backoffMs;
        backoffMs = Math.min(Wire.BACKOFF_MAX_MS, backoffMs * 2);
        logEvento("RELIGAR", String.valueOf(espera));
        System.out.println("[" + id + "] Nova tentativa de ligação em " + espera + " ms...");
        scheduler.schedule(this::conectar, espera, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        religacaoAgendada.set(false);
        this.wsAtual = webSocket;
        webSocket.request(1);

        // Se tiver token de 1.º registo, envia OLA com chave pública e token; senão envia apenas OLA;<id>
        if (tokenRegisto != null && !tokenRegisto.isBlank()) {
            String pubB64 = Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded());
            webSocket.sendText("OLA;" + id + ";" + pubB64 + ";" + tokenRegisto, true);
        } else {
            webSocket.sendText("OLA;" + id, true);
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        bufferRececao.append(data);
        if (last) {
            String msg = bufferRececao.toString();
            bufferRececao.setLength(0);
            processarMensagem(webSocket, msg);
        }
        webSocket.request(1);
        return null;
    }

    /**
     * Interpreta mensagens recebidas do hub: DESAFIO ou STORE.
     */
    private void processarMensagem(WebSocket ws, String msg) {
        try {
            if (msg.startsWith("DESAFIO;")) {
                // Responde ao desafio criptográfico com a assinatura Ed25519
                String nonceB64 = msg.substring(8).trim();
                String assinaturaB64 = assinarDesafio(nonceB64);
                ws.sendText("PROVA;" + id + ";" + assinaturaB64, true);

                // Solicita subscrição ao seu tópico privado e ao canal de controlo
                ws.sendText("SUB;" + Wire.topicReplica(id), true);
                ws.sendText("SUB;" + Wire.TOPICO_CTL, true);

                // Repõe o backoff para o mínimo após sucesso e descarta o token de uso único
                backoffMs = Wire.BACKOFF_MIN_MS;
                tokenRegisto = null;
                logEvento("LIGADO", "Prova enviada ao hub " + uri);
                System.out.println("[" + id + "] Prova enviada e subscrições ativas!");

                // Envia inventário de chunks e inicia temporizador de heartbeats
                enviarInventario(ws);
                iniciarHeartbeats(ws);

            } else if (msg.startsWith("STORE;")) {
                processarStore(ws, msg);
            }
        } catch (Exception e) {
            logEvento("ERRO", e.getMessage());
            System.err.println("[" + id + "] Erro ao processar mensagem: " + e.getMessage());
        }
    }

    /**
     * Assina digitalmente o nonce com a chave privada Ed25519 da réplica.
     * Dados assinados: "tpf-registo|" + id + "|" + nonceB64.
     */
    private String assinarDesafio(String nonceB64) throws Exception {
        byte[] payload = ("tpf-registo|" + id + "|" + nonceB64).getBytes(StandardCharsets.UTF_8);
        Signature sig = Signature.getInstance("Ed25519");
        sig.initSign(keyPair.getPrivate());
        sig.update(payload);
        return Base64.getEncoder().encodeToString(sig.sign());
    }

    /**
     * Inicia o envio periódico de heartbeats a cada 2 segundos.
     */
    private void iniciarHeartbeats(WebSocket ws) {
        if (hbTask != null) hbTask.cancel(false);
        hbTask = scheduler.scheduleAtFixedRate(() -> {
            if (wsAtual != null && !wsAtual.isOutputClosed()) {
                String hb = "PUB;" + Wire.TOPICO_LIDER + ";HB;" + id + ";" + bytesUsados + ";" + quotaBytes;
                wsAtual.sendText(hb, true);
            }
        }, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Envia a lista de identificadores de chunks guardados localmente logo após a ligação.
     */
    private synchronized void enviarInventario(WebSocket ws) throws IOException {
        List<String> ids = new ArrayList<>();
        try (Stream<Path> files = Files.list(pastaDados)) {
            for (Path p : files.toList()) {
                String nome = p.getFileName().toString();
                if (nome.endsWith(".bin")) {
                    ids.add(nome.substring(0, nome.length() - 4));
                }
            }
        }
        String lista = String.join(",", ids);
        ws.sendText("PUB;" + Wire.TOPICO_LIDER + ";INVENTARIO;" + id + ";" + lista, true);
    }

    /**
     * Processa a gravação segura de um chunk recebido via 'STORE;<chunkId>;<hdrB64>;<binB64>':
     * 1. Verifica se já existe em disco (idempotência: responde logo ACK);
     * 2. Verifica se cabe na quota;
     * 3. Valida a integridade SHA-256 do texto cifrado;
     * 4. Escreve atomicamente .bin e .hdr (Temporary File + ATOMIC_MOVE);
     * 5. Envia formalmente o ACK ao líder.
     */
    private synchronized void processarStore(WebSocket ws, String msg) throws Exception {
        String[] parts = msg.split(";", 4);
        if (parts.length != 4) return;

        String chunkId = parts[1];
        byte[] hdrBytes = Base64.getDecoder().decode(parts[2]);
        byte[] binBytes = Base64.getDecoder().decode(parts[3]);

        Path binFile = pastaDados.resolve(chunkId + ".bin");
        Path hdrFile = pastaDados.resolve(chunkId + ".hdr");

        // Idempotência: se já existe, responde com ACK sem reescrever
        if (Files.exists(binFile) && Files.exists(hdrFile)) {
            String hashExistente = sha256Hex(Files.readAllBytes(binFile));
            enviarAck(ws, chunkId, hashExistente);
            return;
        }

        // 1. Verificação de Quota física
        if (bytesUsados + binBytes.length > quotaBytes) {
            logEvento("QUOTA", "Recusado chunk " + chunkId + " (" + binBytes.length + " B)");
            System.out.println("[" + id + "] QUOTA excedida ao tentar guardar " + chunkId.substring(0, 12));
            return;
        }

        // 2. Verificação de Integridade por Endereçamento por Conteúdo
        String hashCalculado = sha256Hex(binBytes);
        if (!hashCalculado.equalsIgnoreCase(chunkId)) {
            logEvento("ERRO", "Hash invalido no STORE para " + chunkId);
            return;
        }

        // 3. Escrita Atómica com ATOMIC_MOVE
        Path tmpBin = pastaDados.resolve(chunkId + ".bin.tmp");
        Files.write(tmpBin, binBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmpBin, binFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        Path tmpHdr = pastaDados.resolve(chunkId + ".hdr.tmp");
        Files.write(tmpHdr, hdrBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmpHdr, hdrFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        bytesUsados += binBytes.length;
        logEvento("STORE", chunkId + " (" + binBytes.length + " B)");
        System.out.println("[" + id + "] Chunk guardado: " + chunkId.substring(0, 12) + "... (" + bytesUsados + "/" + quotaBytes + " B)");

        // 4. Confirmação durável ao líder
        enviarAck(ws, chunkId, hashCalculado);
    }

    /**
     * Envia o ACK formal de durabilidade para o tópico do líder.
     */
    private void enviarAck(WebSocket ws, String chunkId, String sha256) {
        String idAEnviar = FORJAR_ID.isBlank() ? id : FORJAR_ID;
        String hashAEnviar = CORROMPER_ACK ? "0000000000000000000000000000000000000000000000000000000000000000" : sha256;
        ws.sendText("PUB;" + Wire.TOPICO_LIDER + ";ACK;" + idAEnviar + ";" + chunkId + ";" + hashAEnviar, true);
    }

    /**
     * Calcula o hash SHA-256 em hexadecimal.
     */
    private static String sha256Hex(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        logEvento("DESLIGADO", "code=" + statusCode + " reason=" + reason);
        System.out.println("[" + id + "] Desligado do hub (" + statusCode + ": " + reason + ")");
        agendarReligacao();
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        logEvento("ERRO", error.getMessage());
        agendarReligacao();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: java -cp out tpf.ReplicaService <id> <url> <quotaBytes> [token]");
            System.exit(1);
        }
        String id = args[0];
        String url = args[1];
        long quota = Long.parseLong(args[2]);
        String token = args.length >= 4 ? args[3] : null;

        ReplicaService service = new ReplicaService(id, url, quota, token);
        service.iniciar();

        Thread.currentThread().join();
    }
}
