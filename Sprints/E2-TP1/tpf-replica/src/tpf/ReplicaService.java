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
 * Serviço de Réplica Voluntária (tpf-replica).
 * Execução: java -cp out tpf.ReplicaService <id> <url> <quotaBytes> [token]
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

    // Flag opcional para a Experiência 8 (simular hash errado no ACK)
    private static final boolean CORROMPER_ACK = Boolean.getBoolean("tpf.corromperAck");
    // Flag opcional para a Experiência 9 (simular ACK com ID falso)
    private static final String FORJAR_ID = System.getProperty("tpf.forjarId", "");

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
                            .setPermissions(EnumSet.of(
                                    AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA,
                                    AclEntryPermission.READ_ATTRIBUTES, AclEntryPermission.WRITE_ATTRIBUTES,
                                    AclEntryPermission.SYNCHRONIZE
                            )).build();
                    aclView.setAcl(Collections.singletonList(entry));
                }
            }
        } catch (Exception ignored) {}
    }

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

    private synchronized void logEvento(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.write(ficheiroEventos, linha.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Erro ao gravar evento da réplica: " + e.getMessage());
        }
    }

    public void iniciar() {
        conectar();
    }

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

    private void processarMensagem(WebSocket ws, String msg) {
        try {
            if (msg.startsWith("DESAFIO;")) {
                String nonceB64 = msg.substring(8).trim();
                String assinaturaB64 = assinarDesafio(nonceB64);
                ws.sendText("PROVA;" + id + ";" + assinaturaB64, true);

                // O hub só aceita estes SUB depois de validar a PROVA.
                ws.sendText("SUB;" + Wire.topicReplica(id), true);
                ws.sendText("SUB;" + Wire.TOPICO_CTL, true);

                // A partir deste ponto usa-se apenas OLA;<id>.
                backoffMs = Wire.BACKOFF_MIN_MS;
                tokenRegisto = null;
                logEvento("LIGADO", "Prova enviada ao hub " + uri);
                System.out.println("[" + id + "] Prova enviada e subscricoes solicitadas!");

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

    private String assinarDesafio(String nonceB64) throws Exception {
        byte[] payload = ("tpf-registo|" + id + "|" + nonceB64).getBytes(StandardCharsets.UTF_8);
        Signature sig = Signature.getInstance("Ed25519");
        sig.initSign(keyPair.getPrivate());
        sig.update(payload);
        return Base64.getEncoder().encodeToString(sig.sign());
    }

    private void iniciarHeartbeats(WebSocket ws) {
        if (hbTask != null) hbTask.cancel(false);
        hbTask = scheduler.scheduleAtFixedRate(() -> {
            if (wsAtual != null && !wsAtual.isOutputClosed()) {
                String hb = "PUB;" + Wire.TOPICO_LIDER + ";HB;" + id + ";" + bytesUsados + ";" + quotaBytes;
                wsAtual.sendText(hb, true);
            }
        }, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

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

    private synchronized void processarStore(WebSocket ws, String msg) throws Exception {
        // STORE;<chunkId>;<hdrB64>;<binB64>
        String[] parts = msg.split(";", 4);
        if (parts.length != 4) return;

        String chunkId = parts[1];
        byte[] hdrBytes = Base64.getDecoder().decode(parts[2]);
        byte[] binBytes = Base64.getDecoder().decode(parts[3]);

        Path binFile = pastaDados.resolve(chunkId + ".bin");
        Path hdrFile = pastaDados.resolve(chunkId + ".hdr");

        // 5. Se já existe, responde com ACK sem reescrever
        if (Files.exists(binFile) && Files.exists(hdrFile)) {
            String hashExistente = sha256Hex(Files.readAllBytes(binFile));
            enviarAck(ws, chunkId, hashExistente);
            return;
        }

        // 1. Verifica quota
        if (bytesUsados + binBytes.length > quotaBytes) {
            logEvento("QUOTA", "Recusado chunk " + chunkId + " (" + binBytes.length + " B)");
            System.out.println("[" + id + "] QUOTA excedida ao tentar guardar " + chunkId.substring(0, 12));
            return;
        }

        // 2. Verifica integridade SHA-256 do .bin
        String hashCalculado = sha256Hex(binBytes);
        if (!hashCalculado.equalsIgnoreCase(chunkId)) {
            logEvento("ERRO", "Hash invalido no STORE para " + chunkId);
            return;
        }

        // 3. Escreve .bin e .hdr atomicamente
        Path tmpBin = pastaDados.resolve(chunkId + ".bin.tmp");
        Files.write(tmpBin, binBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmpBin, binFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        Path tmpHdr = pastaDados.resolve(chunkId + ".hdr.tmp");
        Files.write(tmpHdr, hdrBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmpHdr, hdrFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        bytesUsados += binBytes.length;
        logEvento("STORE", chunkId + " (" + binBytes.length + " B)");
        System.out.println("[" + id + "] Chunk guardado: " + chunkId.substring(0, 12) + "... (" + bytesUsados + "/" + quotaBytes + " B)");

        // 4. Só depois envia ACK
        enviarAck(ws, chunkId, hashCalculado);
    }

    private void enviarAck(WebSocket ws, String chunkId, String sha256) {
        String idAEnviar = FORJAR_ID.isBlank() ? id : FORJAR_ID;
        String hashAEnviar = CORROMPER_ACK ? "0000000000000000000000000000000000000000000000000000000000000000" : sha256;
        ws.sendText("PUB;" + Wire.TOPICO_LIDER + ";ACK;" + idAEnviar + ";" + chunkId + ";" + hashAEnviar, true);
    }

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

        // Mantém o processo vivo
        Thread.currentThread().join();
    }
}