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

    // Componentes do Sprint 3
    private static ReplicaRegistry registry;
    private static Catalog catalog;
    private static PubSubHub hub;

    // Controlo de envios STORE pendentes por chunk: chunkId -> Set<replicaId>
    private static final Map<String, Set<String>> storesPendentes = new ConcurrentHashMap<>();
    // Réplicas já tentadas por chunk (para excluir nas rondas seguintes): chunkId -> Set<replicaId>
    private static final Map<String, Set<String>> replicasTentadas = new ConcurrentHashMap<>();

    public LeaderBot() {
        initMasterKeyAndStorage();
        recoverLsnFromFile();
        registry = new ReplicaRegistry(LeaderBot::logEvent);
        catalog = new Catalog(BASE_DIR);
        initScheduler();
        initHub();
    }

    private static Path resolveBaseDir() {
        Path p1 = Paths.get("Sprints/E2-TP1/tpf-lider");
        if (Files.isDirectory(p1)) return p1;
        Path p2 = Paths.get("Sprints/E2-TP2/tpf-lider");
        if (Files.isDirectory(p2)) return p2;
        return Paths.get(".");
    }

    // Verifica e gera a master.key (32 bytes) no primeiro arranque e cria a pasta selados/
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
                            .setPermissions(EnumSet.of(
                                    AclEntryPermission.READ_DATA,
                                    AclEntryPermission.READ_ATTRIBUTES,
                                    AclEntryPermission.READ_NAMED_ATTRS,
                                    AclEntryPermission.READ_ACL,
                                    AclEntryPermission.SYNCHRONIZE
                            ))
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

    // Lê o registos.log existente no arranque para continuar o LSN sem repetições nem saltos.
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

    private static synchronized void initScheduler() {
        scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "tpf-scheduler");
            t.setDaemon(true);
            return t;
        });
        // Tarefa periódica para atualizar estados VIVA -> SUSPEITA -> MORTA
        scheduler.scheduleAtFixedRate(() -> registry.atualizarEstados(System.currentTimeMillis()),
                1000, 1000, TimeUnit.MILLISECONDS);
    }

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

    // Regista eventos operacionais em eventos-lider.txt (instante;evento;detalhe).
    private static synchronized void logEvent(String evento, String detalhe) {
        String linha = System.currentTimeMillis() + ";" + evento + ";" + detalhe + "\n";
        try {
            Files.write(BASE_DIR.resolve("eventos-lider.txt"), linha.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Erro ao escrever em eventos-lider.txt: " + e.getMessage());
        }
    }

    // Receção de Mensagens do Tópico 'tpf-lider' (HB, INVENTARIO, ACK)
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

                // Validação 1: ID na mensagem == ID autenticado na ligação
                if (!replicaAutenticada.equals(idMsg)) {
                    logEvent("ACK-REJEITADO", chunkId + " " + idMsg + " id_forjado_por_" + replicaAutenticada);
                    System.err.println("[ACK-REJEITADO] ID forjado: " + idMsg + " na ligação de " + replicaAutenticada);
                    return;
                }

                // Validação 2: Hash SHA-256 == chunkId
                if (!chunkId.equalsIgnoreCase(sha256)) {
                    logEvent("ACK-REJEITADO", chunkId + " " + replicaAutenticada + " hash_invalido");
                    System.err.println("[ACK-REJEITADO] Hash inválido de " + replicaAutenticada + " para " + chunkId);
                    return;
                }

                // Validação 3: Esta réplica recebeu realmente o STORE deste chunk
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

    // Método centralizado de escrita durável (atribui LSN, escreve no log, força fsync e adiciona ao chunk).
    public static synchronized long registarDuravel(String autor, String canal, String texto) {
        currentLsn++;
        long instante = System.currentTimeMillis();
        String textoBase64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        String linha = currentLsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoBase64 + "\n";

        Path logPath = BASE_DIR.resolve("registos.log");
        try (FileChannel channel = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(linha.getBytes(StandardCharsets.UTF_8)));
            channel.force(true); // Força o fsync físico para o disco antes de prosseguir
        } catch (IOException e) {
            throw new RuntimeException("Erro crítico no fsync de registos.log: " + e.getMessage(), e);
        }

        logEvent("REGISTO", "lsn=" + currentLsn + ", autor=" + autor);
        addToOpenChunk(new Chunk.RecordEntry(currentLsn, instante, autor, canal, textoBase64));
        return currentLsn;
    }

    // Adiciona registos ao chunk aberto e gere a selagem por tamanho ou tempo (SELAGEM_MS).
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

    // Sela o chunk aberto, aplica padding, cifra com ChunkCrypto e guarda em selados/.
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

            // Sprint 3: Entra no catálogo e inicia replicação
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

    // Executa uma ronda de colocação do chunk nas réplicas escolhidas pelo Placement
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

        // Agenda verificação ao fim de ACK_TIMEOUT_MS
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

    @Override
    public void onReady(ReadyEvent event) {
        System.out.println("LeaderBot online como: " + event.getJDA().getSelfUser().getAsTag());

        // Flag recomendada pelo docente para evitar exceder o rate limit diário (200 updates/dia) do Discord
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

    // Tratamento de Slash Commands
    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String cmd = event.getName();

        // 1. Verificação de permissões para comandos de administração
        if (isRestrictedCommand(cmd)) {
            Member member = event.getMember();
            if (member == null || !hasAdminRole(member)) {
                event.reply("❌ Acesso recusado: o comando `/" + cmd + "` está restrito a membros com o papel `" + ROLE_ADMIN + "`.")
                        .setEphemeral(true)
                        .queue();
                return;
            }
        }

        // 2. Processamento e validação de argumentos por comando
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

    // Tratamento de mensagens no canal de texto #arquivo
    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        // Ignora mensagens de outros bots (inclusive as do próprio LeaderBot)
        if (event.getAuthor().isBot()) return;

        // Apenas escuta no canal denominado "arquivo"
        if (!CHANNEL_ARQUIVO.equalsIgnoreCase(event.getChannel().getName())) return;

        String conteudo = event.getMessage().getContentRaw();
        String autor = event.getAuthor().getName();

        System.out.println("Mensagem em #" + CHANNEL_ARQUIVO + " detetada: '" + conteudo + "' [Autor: " + autor + "]");

        long assignedLsn = registarDuravel(autor, "#" + CHANNEL_ARQUIVO, conteudo);
        event.getMessage().reply("✅ Mensagem gravada no arquivo durável (LSN: **" + assignedLsn + "**)").queue();
    }

    private boolean isRestrictedCommand(String cmd) {
        return "config".equalsIgnoreCase(cmd)
                || "replica-token".equalsIgnoreCase(cmd)
                || "replica-remover".equalsIgnoreCase(cmd)
                || "rajada".equalsIgnoreCase(cmd);
    }

    private boolean hasAdminRole(Member member) {
        for (Role role : member.getRoles()) {
            if (ROLE_ADMIN.equalsIgnoreCase(role.getName())) return true;
        }
        return false;
    }

    // Leitura segura do Token do Bot (Ambiente -> ficheiro local -> System Property)
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
            JDABuilder.createDefault(token)//inicia coisas do JDA
                    .enableIntents(GatewayIntent.MESSAGE_CONTENT)//permite ler o conteúdo das mensagens
                    .addEventListeners(new LeaderBot())//adiciona o LeaderBot como ouvinte de eventos
                    .build();
        } catch (Exception e) {
            System.err.println("Falha ao iniciar o LeaderBot: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
