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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class LeaderBot extends ListenerAdapter {

    private static final String ROLE_ADMIN = "admin-arquivo";
    private static final String CHANNEL_ARQUIVO = "arquivo";

    // Registo imediato dos 7 Slash Commands ao ficar pronto
    @Override
    public void onReady(ReadyEvent event) {
        System.out.println("LeaderBot online como: " + event.getJDA().getSelfUser().getAsTag());

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
                    Commands.slash("estado", "Mostra o estado do líder, das réplicas e blocos com cópias em falta"),

                    // 3. /config r <n> (usa STRING para validar 'abc' e valores inválidos conforme Critério de Aceitação)
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
                event.reply("«/registar recebido: texto='" + texto + "' — provisório»").queue();
            }

            case "estado" -> {
                event.reply("«/estado recebido: líder online, réplicas conectadas=0, blocos com cópias em falta=0 — provisório»").queue();
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
                event.reply("«/config r recebido: n=" + n + " — provisório»").queue();
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
                event.reply("«/recuperar recebido: de=" + de + ", ate=" + ate + " — ainda não implementado»").queue();
            }

            case "replica-token" -> {
                event.reply("«/replica-token recebido: token gerado='token-provisorio-sprint1' — provisório»").queue();
            }

            case "replica-remover" -> {
                OptionMapping optId = event.getOption("id");
                String id = optId != null ? optId.getAsString().trim() : "";
                if (id.isEmpty()) {
                    event.reply("❌ Argumento inválido: indique o identificador da réplica a remover.").setEphemeral(true).queue();
                    return;
                }
                event.reply("«/replica-remover recebido: id='" + id + "' — provisório»").queue();
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
                event.reply("«/rajada recebido: n=" + n + ", intervaloMs=" + intervaloMs + " — provisório»").queue();
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

        // Resposta provisória no canal a confirmar a deteção
        event.getMessage().reply("«Mensagem em #" + CHANNEL_ARQUIVO + " detetada e recebida provisoriamente: '" + conteudo + "'»").queue();
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

