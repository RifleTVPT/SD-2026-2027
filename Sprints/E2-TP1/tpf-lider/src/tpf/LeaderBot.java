package tpf;

import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.Role;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.events.session.ReadyEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Orquestrador Central e Nó Líder do Arquivo Distribuído (LeaderBot).
 * 
 * PAPEL NO SISTEMA:
 * É o componente central e autoritativo do sistema distribuído:
 * 1. Ponto de Entrada de Dados (Discord Gateway):
 *    - Monitoriza o canal de texto '#arquivo' e processa o Slash Command '/registar';
 *    - Atribui sequencialmente o LSN (Log Sequence Number) a cada novo registo;
 *    - Persiste imediatamente em 'registos.log' com garantia física através de 'channel.force(true)' (fsync).
 * 2. Selagem e Cifra Zero-Knowledge:
 *    - Agrupa registos em memória no chunk aberto;
 *    - Quando atinge CHUNK_MAX_BYTES (64 KB) ou após inatividade de SELAGEM_MS (5 s), sela o chunk;
 *    - Aplica padding nulo (PAD_BYTES = 4 KB), deriva a chave DEK via HKDF-SHA256 da master.key
 *      e cifra em AES-256-GCM gerando ficheiros atómicos em 'selados/<chunkId>.bin' e '.hdr'.
 * 3. Colocação e Replicação Parcial (Sprint 3):
 *    - Regista o chunk no Catalog com estado SELADO;
 *    - Consulta o ReplicaRegistry e utiliza o algoritmo Placement (Rendezvous Hashing)
 *      para selecionar até R = 3 réplicas que não estejam MORTAS e tenham espaço livre suficiente;
 *    - Reserva atómicamente espaço em disco nas réplicas escolhidas;
 *    - Publica a mensagem 'STORE;<chunkId>;<hdrB64>;<binB64>' nos tópicos privados 'tpf-r-<id>' via PubSubHub;
 *    - Aguarda confirmações 'ACK' válidas das réplicas, transitando o chunk para DURAVEL assim que
 *      atinge o quórum de durabilidade: max(W_MIN, floor(R/2) + 1) = 2 confirmações (para R=3);
 *    - Se uma réplica falhar ou expirar ACK_TIMEOUT_MS (4 s), liberta a reserva e agenda novas rondas
 *      (até MAX_RONDAS = 3) para tentar obter as R cópias pretendidas.
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * 1. Thread Main: Inicializa master.key, recupera LSN de registos.log, arranca o agendador e o JDA;
 * 2. Threads Gateway do JDA (WebSocket do Discord): Recebem mensagens de utilizadores (#arquivo e slash commands);
 * 3. Threads Worker do PubSubHub (WebSocket Server): Recebem heartbeats, inventários e ACKs das réplicas;
 * 4. Thread do Agendador Periódico (ScheduledExecutorService "tpf-scheduler"):
 *    - Dispara a selagem por tempo (sealOpenChunk) após 5 segundos;
 *    - Avalia transições de estado das réplicas (VIVA -> SUSPEITA -> MORTA) a cada 1 segundo;
 *    - Gere timeouts de ACK e rondas subsequentes de colocação.
 * 
 * HIERARQUIA DE SINCRONIZAÇÃO (Prevenção de Race Conditions e Deadlocks):
 * - Os métodos de escrita no log e controlo do chunk aberto (registarDuravel, addToOpenChunk, sealOpenChunk)
 *   são estáticos e 'synchronized', serializando adições concorrentes de LSN;
 * - ReplicaRegistry e Catalog possuem os seus próprios monitores internos 'synchronized(this)',
 *   sendo acedidos ordenadamente sem dependências circulares.
 */
public class LeaderBot extends ListenerAdapter {

    private static final String ROLE_ADMIN = "admin-arquivo";
    private static final String CHANNEL_ARQUIVO = "arquivo";

    private static final Path BASE_DIR = resolveBaseDir();
    private static long currentLsn = 0; // Controlo do último número de sequência do log
    private static ChunkCrypto crypto;  // Instância de cifra para selar os blocos

    // Controlo em memória do Chunk Aberto e agendamento de selagem por tempo
    private static final List<Chunk.RecordEntry> openChunkRecords = new ArrayList<>();
    private static long openChunkByteSize = 0;
    private static ScheduledExecutorService scheduler;
    private static ScheduledFuture<?> sealTask;

    // Componentes centrais do Sprint 3
    private static ReplicaRegistry registry;
    private static Catalog catalog;
    private static PubSubHub hub;

    // Controlo de envios STORE pendentes por chunk: chunkId -> Set<replicaId>
    private static final Map<String, Set<String>> storesPendentes = new ConcurrentHashMap<>();
    // Réplicas já tentadas por chunk (para excluir nas rondas seguintes): chunkId -> Set<replicaId>
    private static final Map<String, Set<String>> replicasTentadas = new ConcurrentHashMap<>();

    /**
     * Construtor do LeaderBot. Inicializa chaves, recupera estado e arranca os serviços.
     */
    public LeaderBot() {
        initMasterKeyAndStorage();
        recoverLsnFromFile();
        registry = new ReplicaRegistry(LeaderBot::logEvent);
        catalog = new Catalog(BASE_DIR);
        initScheduler();
        initHub();
    }

    /**
     * Localiza a pasta base do projeto (suporta execução na raiz do repositório ou dentro da subpasta).
     */
    private static Path resolveBaseDir() {
        Path p1 = Paths.get("Sprints/E2-TP1/tpf-lider");
        if (Files.isDirectory(p1)) return p1;
        Path p2 = Paths.get("Sprints/E2-TP2/tpf-lider");
        if (Files.isDirectory(p2)) return p2;
        return Paths.get(".");
    }

    /**
     * Inicializa o motor de cifra: lê a master.key (32 bytes) existente ou gera uma nova no 1.º arranque.
     * Restringe as permissões do ficheiro para leitura exclusiva pelo dono.
     */
    private static synchronized void initMasterKeyAndStorage() {
        try {
            Files.createDirectories(BASE_DIR.resolve("selados"));
            Path mkPath = BASE_DIR.resolve("master.key");
            byte[] mkBytes;

            if (!Files.exists(mkPath)) {
                mkBytes = new byte[32];
                new SecureRandom().nextBytes(mkBytes);
                Files.write(mkPath, mkBytes, StandardOpenOption.CREATE_NEW);
                restrictToOwnerOnly(mkPath);
                logEvent("MASTER_KEY", "Gerada nova master.key de 32 bytes");
            } else {
                mkBytes = Files.readAllBytes(mkPath);
                if (mkBytes.length != 32) {
                    System.err.println("ERRO FATAL: master.key tem " + mkBytes.length + " bytes (esperado 32).");
                    System.exit(1);
                }
            }
            crypto = new ChunkCrypto(mkBytes, 1);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao inicializar master.key ou pastas: " + e.getMessage(), e);
        }
    }

    /**
     * Restringe as permissões da chave mestra apenas ao dono (POSIX ou ACL Windows).
     */
    private static void restrictToOwnerOnly(Path path) {
        try {
            if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("r--------"));
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
                } else {
                    File f = path.toFile();
                    f.setReadOnly();
                }
            }
        } catch (Exception e) {
            System.err.println("Aviso ao ajustar permissões de master.key: " + e.getMessage());
        }
    }

    /**
     * Recupera o último LSN a partir de 'registos.log' no arranque para evitar duplicações de sequência.
     */
    private static synchronized void recoverLsnFromFile() {
        Path logPath = BASE_DIR.resolve("registos.log");
        if (!Files.exists(logPath)) return;
        try {
            List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isBlank()) continue;
                String[] parts = line.split(";", 2);
                if (parts.length > 0) {
                    try {
                        long lsn = Long.parseLong(parts[0].trim());
                        if (lsn > currentLsn) currentLsn = lsn;
                    } catch (NumberFormatException ignored) {}
                }
            }
            System.out.println("Recuperação concluída. LSN atual a partir de registos.log: " + currentLsn);
        } catch (IOException e) {
            System.err.println("Erro ao ler registos.log: " + e.getMessage());
        }
    }

    /**
     * Inicializa o pool de agendamento de tarefas em segundo plano (selagem por tempo e avaliação de heartbeats).
     */
    private static synchronized void initScheduler() {
        scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "tpf-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> registry.atualizarEstados(System.currentTimeMillis()),
                1000, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Arranca o servidor WebSocket do PubSubHub embutido no processo do Líder.
     */
    private static void initHub() {
        hub = new PubSubHub(
                Wire.PORTO,
                BASE_DIR,
                LeaderBot::processarMensagemLider,
                (id, ev) -> registry.registarOuAtualizar(id),
                LeaderBot::logEvent
        );
        hub.start();
    }

    /**
     * Regista eventos operacionais em 'eventos-lider.txt' no formato: instante;evento;detalhe.
     */
    private static synchronized void logEvent(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.write(BASE_DIR.resolve("eventos-lider.txt"), linha.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Erro ao escrever em eventos-lider.txt: " + e.getMessage());
        }
    }

    /**
     * Despacha mensagens publicadas pelas réplicas no tópico 'tpf-lider': HB, INVENTARIO e ACK.
     * 
     * Onde é utilizado: Callback registada no PubSubHub ao receber mensagens no tópico do líder.
     */
    private static void processarMensagemLider(String replicaAutenticada, String payload) {
        try {
            if (payload.startsWith("HB;")) {
                // HB;<id>;<usados>;<quota>
                String[] p = payload.split(";");
                if (p.length >= 4 && replicaAutenticada.equals(p[1])) {
                    long usados = Long.parseLong(p[2]);
                    long quota = Long.parseLong(p[3]);
                    registry.onHeartbeat(replicaAutenticada, usados, quota);
                }
            } else if (payload.startsWith("INVENTARIO;")) {
                // INVENTARIO;<id>;<chunkId1>,<chunkId2>,...
                String[] p = payload.split(";", 3);
                if (p.length >= 2 && replicaAutenticada.equals(p[1])) {
                    String lista = (p.length == 3) ? p[2] : "";
                    int qtd = lista.isBlank() ? 0 : lista.split(",").length;
                    System.out.println("[INVENTARIO] Réplica " + replicaAutenticada + " reportou " + qtd + " chunks.");
                    logEvent("INVENTARIO", replicaAutenticada + " chunks=" + qtd);
                }
            } else if (payload.startsWith("ACK;")) {
                // ACK;<id>;<chunkId>;<sha256>
                String[] p = payload.split(";");
                if (p.length < 4) return;
                String idMsg = p[1];
                String chunkId = p[2];
                String sha256 = p[3];

                // Validação 1: O identificador na mensagem tem de coincidir com a sessão WebSocket autenticada
                if (!replicaAutenticada.equals(idMsg)) {
                    logEvent("ACK-REJEITADO", chunkId + " " + idMsg + " id_forjado_por_" + replicaAutenticada);
                    System.err.println("[ACK-REJEITADO] ID forjado: " + idMsg + " na ligação de " + replicaAutenticada);
                    return;
                }

                // Validação 2: O hash reportado tem de ser exatamente o chunkId
                if (!chunkId.equalsIgnoreCase(sha256)) {
                    logEvent("ACK-REJEITADO", chunkId + " " + replicaAutenticada + " hash_invalido");
                    System.err.println("[ACK-REJEITADO] Hash inválido de " + replicaAutenticada + " para " + chunkId);
                    return;
                }

                // Validação 3: Esta réplica tem de constar da lista de envios STORE pendentes deste chunk
                Set<String> pendentes = storesPendentes.get(chunkId);
                if (pendentes == null || !pendentes.remove(replicaAutenticada)) {
                    logEvent("ACK-REJEITADO", chunkId + " " + replicaAutenticada + " store_nao_solicitado");
                    System.err.println("[ACK-REJEITADO] ACK não solicitado de " + replicaAutenticada + " para " + chunkId);
                    return;
                }

                Catalog.EntradaCatalogo entrada = catalog.get(chunkId);
                if (entrada != null) {
                    registry.confirmarReserva(replicaAutenticada, entrada.tamanho);
                    logEvent("ACK", chunkId + " " + replicaAutenticada);
                    boolean tornouDuravel = catalog.confirmar(chunkId, replicaAutenticada);
                    if (tornouDuravel) {
                        logEvent("DURAVEL", chunkId);
                        System.out.println("[DURAVEL] Chunk " + chunkId.substring(0, 12) + "... atingiu quórum!");
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Erro ao processar mensagem de " + replicaAutenticada + ": " + e.getMessage());
        }
    }

    /**
     * Gravação Atómica e Durável de um novo registo:
     * 1. Incrementa o LSN;
     * 2. Formata a linha delimitada por ';';
     * 3. Grava no FileChannel de 'registos.log' e executa channel.force(true) (fsync físico);
     * 4. Regista evento 'REGISTO' em eventos-lider.txt;
     * 5. Acrescenta ao chunk aberto em memória.
     * 
     * Onde é utilizado: onMessageReceived (#arquivo) e onSlashCommandInteraction (/registar e /rajada).
     */
    public static synchronized long registarDuravel(String autor, String canal, String texto) {
        currentLsn++;
        long instante = System.currentTimeMillis();
        String textoBase64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        String linha = currentLsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoBase64 + "\n";

        Path logPath = BASE_DIR.resolve("registos.log");
        try (FileChannel channel = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(linha.getBytes(StandardCharsets.UTF_8)));
            channel.force(true); // fsync obrigatório
        } catch (IOException e) {
            throw new RuntimeException("Erro crítico no fsync de registos.log: " + e.getMessage(), e);
        }

        logEvent("REGISTO", "lsn=" + currentLsn + ", autor=" + autor);
        addToOpenChunk(new Chunk.RecordEntry(currentLsn, instante, autor, canal, textoBase64));
        return currentLsn;
    }

    /**
     * Adiciona um registo ao lote aberto em memória e verifica se atingiu o limite de tamanho (64 KB).
     * Se for o 1.º registo do lote, agenda a selagem automática por tempo (5 s).
     */
    private static synchronized void addToOpenChunk(Chunk.RecordEntry entry) {
        if (openChunkRecords.isEmpty()) {
            sealTask = scheduler.schedule(LeaderBot::sealOpenChunk, Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }

        openChunkRecords.add(entry);
        openChunkByteSize += entry.toLine().getBytes(StandardCharsets.UTF_8).length + 1;

        if (openChunkByteSize >= Wire.CHUNK_MAX_BYTES) {
            if (sealTask != null) sealTask.cancel(false);
            sealOpenChunk();
        }
    }

    /**
     * Sela o chunk aberto:
     * 1. Serializa com padding de 4 KB (Zero-Knowledge);
     * 2. Cifra em AES-256-GCM com HKDF gerando o identificador chunkId;
     * 3. Persiste atomicamente em 'selados/<chunkId>.bin' e '.hdr' com ATOMIC_MOVE;
     * 4. Regista evento 'SELADO' em eventos-lider.txt;
     * 5. Regista no Catalog e inicia a ronda 1 de replicação pelas réplicas.
     */
    private static synchronized void sealOpenChunk() {
        if (openChunkRecords.isEmpty()) return;

        if (sealTask != null) {
            sealTask.cancel(false);
            sealTask = null;
        }

        try {
            long lsnDe = openChunkRecords.get(0).lsn;
            long lsnAte = openChunkRecords.get(openChunkRecords.size() - 1).lsn;

            byte[] serialized = Chunk.serialize(openChunkRecords, Wire.PAD_BYTES);
            ChunkCrypto.Sealed sealed = crypto.seal(serialized);

            Path seladosDir = BASE_DIR.resolve("selados");
            Path tmpBin = seladosDir.resolve(sealed.id + ".bin.tmp");
            Path binFile = seladosDir.resolve(sealed.id + ".bin");
            Files.write(tmpBin, sealed.ciphertext, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpBin, binFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            String headerContent = sealed.keyVersion + ";" +
                    Base64.getEncoder().encodeToString(sealed.salt) + ";" +
                    Base64.getEncoder().encodeToString(sealed.nonce);
            Path tmpHdr = seladosDir.resolve(sealed.id + ".hdr.tmp");
            Path hdrFile = seladosDir.resolve(sealed.id + ".hdr");
            Files.write(tmpHdr, headerContent.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpHdr, hdrFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            long tamanho = sealed.ciphertext.length;
            logEvent("SELADO", "chunkId=" + sealed.id + ", lsnDe=" + lsnDe + ", lsnAte=" + lsnAte + ", tamanho=" + tamanho);
            System.out.println("CHUNK SELADO: " + sealed.id + " [LSN " + lsnDe + ".." + lsnAte + "]");

            openChunkRecords.clear();
            openChunkByteSize = 0;

            catalog.adicionar(sealed.id, lsnDe, lsnAte, tamanho);
            String hdrB64 = Base64.getEncoder().encodeToString(headerContent.getBytes(StandardCharsets.UTF_8));
            String binB64 = Base64.getEncoder().encodeToString(sealed.ciphertext);

            replicasTentadas.put(sealed.id, ConcurrentHashMap.newKeySet());
            storesPendentes.put(sealed.id, ConcurrentHashMap.newKeySet());
            executarRondaColocacao(sealed.id, tamanho, hdrB64, binB64, 1);
        } catch (Exception e) {
            System.err.println("Erro ao selar chunk: " + e.getMessage());
            logEvent("ERRO", "Falha ao selar chunk: " + e.getMessage());
        }
    }

    /**
     * Executa uma ronda de distribuição de um chunk pelas réplicas:
     * - Calcula cópias em falta (R - confirmadas);
     * - Escolhe réplicas via reserveAndChoose excluindo as já tentadas;
     * - Envia STORE via WebSocket no tópico 'tpf-r-<id>';
     * - Agenda verificação após ACK_TIMEOUT_MS (4 s) para libertar reservas pendentes e avançar ronda.
     */
    private static void executarRondaColocacao(String chunkId, long tamanho, String hdrB64, String binB64, int ronda) {
        Set<String> confirmadas = catalog.copias(chunkId);
        int emFalta = Wire.R - confirmadas.size();
        if (emFalta <= 0) return;

        if (ronda > Wire.MAX_RONDAS) {
            if (confirmadas.size() < Wire.R) {
                logEvent("SUBREPLICADO", chunkId + " " + confirmadas.size() + "/" + Wire.R);
            }
            return;
        }

        logEvent("RONDA", chunkId + " " + ronda);
        Set<String> tentadas = replicasTentadas.computeIfAbsent(chunkId, k -> ConcurrentHashMap.newKeySet());
        List<String> escolhidas = registry.reserveAndChoose(chunkId, tamanho, emFalta, tentadas);

        if (confirmadas.size() + escolhidas.size() < Wire.R) {
            logEvent("SUBREPLICADO", chunkId + " " + (confirmadas.size() + escolhidas.size()) + "/" + Wire.R);
        }

        if (escolhidas.isEmpty()) return;

        Set<String> pendentes = storesPendentes.computeIfAbsent(chunkId, k -> ConcurrentHashMap.newKeySet());
        String msgStore = "STORE;" + chunkId + ";" + hdrB64 + ";" + binB64;

        for (String repId : escolhidas) {
            tentadas.add(repId);
            pendentes.add(repId);
            logEvent("STORE", chunkId + " -> " + repId);
            hub.publish(Wire.topicReplica(repId), msgStore);
        }

        scheduler.schedule(() -> {
            for (String repId : escolhidas) {
                if (pendentes.remove(repId)) {
                    registry.libertar(repId, tamanho);
                }
            }
            Set<String> atuais = catalog.copias(chunkId);
            if (atuais.size() < Wire.R) {
                executarRondaColocacao(chunkId, tamanho, hdrB64, binB64, ronda + 1);
            }
        }, Wire.ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Regista os 7 Slash Commands na nuvem do Discord quando o bot fica online.
     * Utiliza a flag -Dtpf.registarComandos=true para evitar estourar o limite de 200 pedidos/dia da API do Discord.
     */
    @Override
    public void onReady(ReadyEvent event) {
        System.out.println("LeaderBot online como: " + event.getJDA().getSelfUser().getAsTag());

        boolean registar = Boolean.getBoolean("tpf.registarComandos")
                || Boolean.parseBoolean(System.getenv().getOrDefault("REGISTAR_COMANDOS", "false"));

        if (!registar) {
            System.out.println("ℹ️ Registo de Slash Commands ignorado (a usar comandos em cache do Discord para poupar plafond).");
            System.out.println("ℹ️ Para forçar atualização de comandos, arranca com: -Dtpf.registarComandos=true");
            return;
        }

        List<Guild> guilds = event.getJDA().getGuilds();
        if (guilds.isEmpty()) {
            System.out.println("AVISO: O bot ainda não pertence a nenhum servidor. Adiciona o bot usando o URL de OAuth2!");
            return;
        }

        for (Guild guild : guilds) {
            System.out.println("A registar Slash Commands na Guild: " + guild.getName() + " (ID: " + guild.getId() + ")...");
            guild.updateCommands().addCommands(
                    // 1. /registar <texto>
                    Commands.slash("registar", "Regista uma mensagem no log do líder")
                            .addOption(OptionType.STRING, "texto", "Texto da mensagem a registar", true),

                    // 2. /estado
                    Commands.slash("estado", "Mostra o estado do líder, das réplicas e blocos"),

                    // 3. /config r <n>
                    Commands.slash("config", "Configuração do sistema (ex: fator de replicação R)")
                            .addSubcommands(new SubcommandData("r", "Define o fator de replicação R")
                                    .addOption(OptionType.STRING, "n", "Novo valor para o fator R (inteiro >= 1)", true)),

                    // 4. /recuperar <de> <ate>
                    Commands.slash("recuperar", "Recupera os registos num intervalo de LSN")
                            .addOption(OptionType.INTEGER, "de", "LSN inicial", true)
                            .addOption(OptionType.INTEGER, "ate", "LSN final", true),

                    // 5. /replica-token
                    Commands.slash("replica-token", "Gera um token de uso único para registo de réplica"),

                    // 6. /replica-remover <id>
                    Commands.slash("replica-remover", "Remove uma réplica do sistema")
                            .addOption(OptionType.STRING, "id", "Identificador da réplica (ex: A, B)", true),

                    // 7. /rajada <n> <intervaloMs>
                    Commands.slash("rajada", "Injeta uma rajada de registos sintéticos para testes de carga")
                            .addOption(OptionType.INTEGER, "n", "Número de registos a injetar", true)
                            .addOption(OptionType.INTEGER, "intervaloms", "Intervalo entre registos em milissegundos", true)
            ).queue(
                    success -> System.out.println("Slash Commands registados com sucesso na Guild: " + guild.getName()),
                    error -> System.err.println("Erro ao registar comandos na Guild " + guild.getName() + ": " + error.getMessage())
            );
        }
    }

    /**
     * Interceta e executa as interações de Slash Commands enviadas pelos utilizadores do Discord.
     */
    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String cmd = event.getName();

        // 1. Controlo de permissões RBAC para comandos administrativos
        if (isRestrictedCommand(cmd)) {
            Member member = event.getMember();
            if (member == null || !hasAdminRole(member)) {
                event.reply("❌ Acesso recusado: o comando `/" + cmd + "` está restrito a membros com o papel `" + ROLE_ADMIN + "`.")
                        .setEphemeral(true)
                        .queue();
                return;
            }
        }

        // 2. Execução do comando solicitado
        switch (cmd) {
            case "registar" -> {
                OptionMapping optTexto = event.getOption("texto");
                String texto = optTexto != null ? optTexto.getAsString().trim() : "";
                if (texto.isEmpty()) {
                    event.reply("❌ Argumento inválido: o texto não pode estar vazio.").setEphemeral(true).queue();
                    return;
                }
                long assignedLsn = registarDuravel(event.getUser().getName(), "discord-slash", texto);
                event.reply("✅ Registo gravado com sucesso! LSN atribuído: **" + assignedLsn + "**").queue();
            }

            case "estado" -> {
                registry.atualizarEstados(System.currentTimeMillis());
                List<ReplicaRegistry.InfoReplica> reps = registry.snapshot();
                Collection<Catalog.EntradaCatalogo> chunks = catalog.todas();

                int selados = 0, duraveis = 0, subreplicados = 0;
                Map<String, Integer> chunksPorReplica = new HashMap<>();

                for (Catalog.EntradaCatalogo c : chunks) {
                    if (c.estado == Catalog.EstadoChunk.SELADO) selados++;
                    else duraveis++;
                    if (c.replicas.size() < Wire.R) subreplicados++;
                    for (String rId : c.replicas) {
                        chunksPorReplica.merge(rId, 1, Integer::sum);
                    }
                }

                StringBuilder sb = new StringBuilder("📊 **Estado do Sistema (R=" + Wire.R + ", Quórum=" + Wire.quorumDuravel(Wire.R) + ")**\n");
                sb.append("**Chunks:** Total=").append(chunks.size())
                  .append(" | `SELADO`=").append(selados)
                  .append(" | `DURAVEL`=").append(duraveis)
                  .append(" | Subreplicados=").append(subreplicados).append("\n\n");

                sb.append("**Réplicas (" + reps.size() + "):**\n");
                if (reps.isEmpty()) {
                    sb.append("_Nenhuma réplica ativa._\n");
                } else {
                    for (ReplicaRegistry.InfoReplica r : reps) {
                        int nChunks = chunksPorReplica.getOrDefault(r.id, 0);
                        sb.append("- **").append(r.id).append("**: `").append(r.monitor.getEstado())
                          .append("` | Ocupação: ").append(r.usados).append("/").append(r.quota).append(" B")
                          .append(" (Reservados: ").append(r.reservados).append(" B)")
                          .append(" | Chunks: ").append(nChunks).append("\n");
                    }
                }
                event.reply(sb.toString()).queue();
            }

            case "config" -> {
                String sub = event.getSubcommandName();
                String rawVal = null;
                if ("r".equalsIgnoreCase(sub)) {
                    OptionMapping optN = event.getOption("n");
                    if (optN != null) rawVal = optN.getAsString().trim();
                } else {
                    OptionMapping optR = event.getOption("r");
                    if (optR != null) rawVal = optR.getAsString().trim();
                }

                if (rawVal == null || rawVal.isEmpty()) {
                    event.reply("❌ Argumento ausente: indique o valor de R.").setEphemeral(true).queue();
                    return;
                }

                long n;
                try {
                    n = Long.parseLong(rawVal);
                } catch (NumberFormatException e) {
                    event.reply("❌ Argumento inválido: '" + rawVal + "' não é um número inteiro válido. O fator R deve ser >= 1.").setEphemeral(true).queue();
                    return;
                }

                if (n <= 0) {
                    event.reply("❌ Argumento inválido: o fator de replicação R deve ser um número inteiro >= 1.").setEphemeral(true).queue();
                    return;
                }
                event.reply("«/config r recebido: n=" + n + " — provisório (convergência no Sprint 4)»").queue();
            }

            case "recuperar" -> {
                OptionMapping optDe = event.getOption("de");
                OptionMapping optAte = event.getOption("ate");
                if (optDe == null || optAte == null) {
                    event.reply("❌ Argumentos ausentes: indique 'de' e 'ate'.").setEphemeral(true).queue();
                    return;
                }
                long de = optDe.getAsLong();
                long ate = optAte.getAsLong();

                if (de < 0 || ate < 0) {
                    event.reply("❌ Argumento inválido: os valores de LSN devem ser não-negativos.").setEphemeral(true).queue();
                    return;
                }
                if (de > ate) {
                    event.reply("❌ Argumento inválido: 'de' (" + de + ") não pode ser maior que 'ate' (" + ate + ").").setEphemeral(true).queue();
                    return;
                }
                event.reply("«/recuperar recebido: de=" + de + ", ate=" + ate + " — provisório (Sprint 4)»").queue();
            }

            case "replica-token" -> {
                String token = hub.gerarToken();
                event.reply("🔑 **Token de registo gerado (uso único, válido por 10 min):**\n`" + token + "`")
                        .setEphemeral(true).queue();
            }

            case "replica-remover" -> {
                OptionMapping optId = event.getOption("id");
                String id = optId != null ? optId.getAsString().trim() : "";
                if (id.isEmpty()) {
                    event.reply("❌ Argumento inválido: indique o identificador da réplica a remover.").setEphemeral(true).queue();
                    return;
                }
                registry.removerReplica(id);
                boolean existia = hub.revogarReplica(id);
                logEvent("REPLICA-REMOVIDA", id);
                event.reply(existia
                        ? "🗑️ Réplica **" + id + "** removida de `replicas.txt`, desligada e excluída da colocação."
                        : "⚠️ Réplica **" + id + "** não estava em `replicas.txt`, mas foi marcada como excluída.").queue();
            }

            case "rajada" -> {
                OptionMapping optN = event.getOption("n");
                OptionMapping optIntervalo = event.getOption("intervaloms");
                if (optN == null || optIntervalo == null) {
                    event.reply("❌ Argumentos ausentes: indique 'n' e 'intervaloMs'.").setEphemeral(true).queue();
                    return;
                }
                long n = optN.getAsLong();
                long intervaloMs = optIntervalo.getAsLong();

                if (n <= 0) {
                    event.reply("❌ Argumento inválido: 'n' deve ser maior que 0.").setEphemeral(true).queue();
                    return;
                }
                if (intervaloMs < 0) {
                    event.reply("❌ Argumento inválido: 'intervaloMs' não pode ser negativo.").setEphemeral(true).queue();
                    return;
                }
                String autor = event.getUser().getName();
                event.reply("🚀 Rajada de " + n + " registos iniciada...").queue();
                new Thread(() -> {
                    for (int i = 1; i <= n; i++) {
                        String payload = "Registo sintético #" + i + " " + "X".repeat(200);
                        registarDuravel(autor, "rajada", payload);
                        if (intervaloMs > 0) {
                            try { Thread.sleep(intervaloMs); } catch (InterruptedException ignored) {}
                        }
                    }
                    System.out.println("Rajada de " + n + " registos concluída com sucesso.");
                    event.getHook().sendMessage("✅ Rajada de **" + n + "** registos concluída!").queue();
                }).start();
            }

            default -> event.reply("Comando desconhecido: " + cmd).setEphemeral(true).queue();
        }
    }

    /**
     * Interceta mensagens de utilizadores humanos publicadas no canal de texto '#arquivo'.
     */
    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;
        if (!CHANNEL_ARQUIVO.equalsIgnoreCase(event.getChannel().getName())) return;

        String conteudo = event.getMessage().getContentRaw();
        String autor = event.getAuthor().getName();

        System.out.println("Mensagem em #" + CHANNEL_ARQUIVO + " detetada: '" + conteudo + "' [Autor: " + autor + "]");

        long assignedLsn = registarDuravel(autor, "#" + CHANNEL_ARQUIVO, conteudo);
        event.getMessage().reply("✅ Mensagem gravada no arquivo durável (LSN: **" + assignedLsn + "**)").queue();
    }

    /**
     * Identifica comandos restritos que exigem o papel 'admin-arquivo'.
     */
    private boolean isRestrictedCommand(String cmd) {
        return "config".equalsIgnoreCase(cmd)
                || "replica-token".equalsIgnoreCase(cmd)
                || "replica-remover".equalsIgnoreCase(cmd)
                || "rajada".equalsIgnoreCase(cmd);
    }

    /**
     * Verifica se o membro solicitante possui o papel 'admin-arquivo'.
     */
    private boolean hasAdminRole(Member member) {
        for (Role role : member.getRoles()) {
            if (ROLE_ADMIN.equalsIgnoreCase(role.getName())) return true;
        }
        return false;
    }

    /**
     * Procura o token do Bot nas várias origens permitidas por ordem de precedência:
     * 1. Argumentos da CLI;
     * 2. Variável de ambiente DISCORD_TOKEN ou BOT_TOKEN;
     * 3. Propriedade -Dtpf.token;
     * 4. Ficheiro token.txt ou bot.token.
     */
    private static String resolveToken(String[] args) {
        if (args.length > 0 && !args[0].isBlank()) return args[0].trim();

        String envToken = System.getenv("DISCORD_TOKEN");
        if (envToken != null && !envToken.isBlank()) return envToken.trim();

        String envBotToken = System.getenv("BOT_TOKEN");
        if (envBotToken != null && !envBotToken.isBlank()) return envBotToken.trim();

        String propToken = System.getProperty("tpf.token");
        if (propToken != null && !propToken.isBlank()) return propToken.trim();

        File[] locais = new File[] {
                new File("token.txt"),
                new File("bot.token"),
                new File("Sprints/E2-TP1/tpf-lider/token.txt"),
                new File("Sprints/E2-TP1/tpf-lider/bot.token"),
                new File(".env")
        };

        for (File f : locais) {
            if (f.exists()) {
                try {
                    String str = Files.readString(f.toPath(), StandardCharsets.UTF_8).trim();
                    if (!str.isBlank()) return str;
                } catch (IOException ignored) {}
            }
        }

        return null;
    }

    /**
     * Ponto de entrada da aplicação Líder (LeaderBot).
     */
    public static void main(String[] args) {
        String token = resolveToken(args);
        if (token == null) {
            System.err.println("ERRO: Token do bot não encontrado!");
            System.err.println("Configura o token através de uma das opções seguintes:");
            System.err.println("  1. Ficheiro 'token.txt' na raiz ou em Sprints/E2-TP1/tpf-lider/token.txt");
            System.err.println("  2. Variável de ambiente: DISCORD_TOKEN");
            System.err.println("  3. Argumento da linha de comandos: java ... tpf.LeaderBot <TOKEN>");
            System.err.println("  4. Propriedade de sistema: -Dtpf.token=<TOKEN>");
            System.exit(1);
        }

        try {
            System.out.println("A iniciar LeaderBot...");
            JDABuilder.createDefault(token)
                    .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                    .addEventListeners(new LeaderBot())
                    .build();
        } catch (Exception e) {
            System.err.println("Falha ao iniciar o LeaderBot: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
