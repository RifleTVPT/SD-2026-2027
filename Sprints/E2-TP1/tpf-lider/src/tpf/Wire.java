package tpf;

/**
 * Constantes Globais de Protocolo e Configuração do Arquivo Distribuído (TPF).
 * 
 * PAPEL NO SISTEMA:
 * Define os parâmetros fundamentais de comunicação, temporização e quóruns partilhados
 * entre o Líder (LeaderBot / PubSubHub) e as Réplicas Voluntárias (ReplicaService).
 * Todos os valores possuem predefinições robustas para os testes laboratoriais mas podem
 * ser sobrepostos em runtime via propriedades de sistema Java (-Dtpf.<prop>=<val>).
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - Classe utilitária imutável (final com construtor privado).
 * - Não mantém estado mutável.
 * - Todos os campos são 'public static final', garantindo visibilidade e thread-safety absoluta
 *   para todas as threads do sistema (JDA, WebSocket, Agendadores, etc.).
 * 
 * ONDE É UTILIZADA:
 * - LeaderBot: limites de chunks, quóruns de ACK, nomes de tópicos.
 * - PubSubHub: porto do WebSocket, validade de tokens e tópicos autorizados.
 * - ReplicaRegistry / HeartbeatMonitor: limites de tempo SUSPEITA e MORTA, margem de segurança.
 * - ReplicaService: períodos de heartbeat, backoff exponencial e validação de tópicos.
 */
public final class Wire {

    // =========================================================================
    // 1. Parâmetros de Selagem e Blocos Criptográficos (Sprint 2)
    // =========================================================================

    /**
     * Tamanho máximo em bytes do conteúdo de um chunk aberto antes de forçar a selagem imediata.
     * Predefinição: 64 KB (65 536 bytes).
     * Onde é utilizado: LeaderBot.verificarLimiteTamanhoOuTempo() ao receber novas mensagens.
     */
    public static final long CHUNK_MAX_BYTES = Long.parseLong(System.getProperty("tpf.chunkMax", "65536"));

    /**
     * Múltiplo de alinhamento em bytes para preenchimento com zeros (Zero-Knowledge padding).
     * Oculta das réplicas o comprimento real dos registos úteis.
     * Predefinição: 4 KB (4 096 bytes).
     * Onde é utilizado: Chunk.serialize() e LeaderBot.selarChunkAtual().
     */
    public static final int PAD_BYTES = Integer.parseInt(System.getProperty("tpf.pad", "4096"));

    /**
     * Tempo limite de inatividade em milissegundos para selar um chunk aberto por tempo.
     * Predefinição: 5 000 ms (5 segundos).
     * Onde é utilizado: LeaderBot.agendarSelagemPorTempo().
     */
    public static final long SELAGEM_MS = Long.parseLong(System.getProperty("tpf.selagem", "5000"));

    // =========================================================================
    // 2. Parâmetros de Replicação, Quórum e Colocação (Sprint 3)
    // =========================================================================

    /**
     * Fator de Replicação Alvo (R): Número de réplicas distintas que devem guardar cada chunk.
     * Predefinição: 3.
     * Onde é utilizado: LeaderBot.distribuirChunkReplicas() e Catalog.confirmar().
     */
    public static final int R = Integer.getInteger("tpf.r", 3);

    /**
     * Fator de Quórum W (legado/informativo).
     * Predefinição: 2.
     */
    public static final int W = Integer.getInteger("tpf.w", 2);

    /**
     * Quórum mínimo absoluto de confirmações (W_MIN) para evitar durabilidade com 1 só cópia.
     * Predefinição: 2.
     * Onde é utilizado: quorumDuravel(int r).
     */
    public static final int W_MIN = Integer.getInteger("tpf.wMin", 2);

    /**
     * Expoente de ponderação alfa para o algoritmo de colocação Rendezvous Hashing:
     * - alfa = 0.0: Colocação puramente uniforme.
     * - alfa = 1.0: Colocação proporcional ao espaço livre efetivo.
     * - alfa = 2.0: Colocação com forte preferência por réplicas de grande capacidade.
     * Onde é utilizado: Placement.score() e PlacementExperiment.
     */
    public static final double ALFA = Double.parseDouble(System.getProperty("tpf.alfa", "1.0"));

    /**
     * Margem de segurança em bytes para além do tamanho do chunk ao selecionar uma réplica.
     * Impede que réplicas fiquem 100% cheias no limite exato da sua quota.
     * Predefinição: 64 KB (65 536 bytes).
     * Onde é utilizado: Placement.choose().
     */
    public static final long MARGEM_BYTES = Long.getLong("tpf.margem", 65536L);

    // =========================================================================
    // 3. Temporizadores de Heartbeat e Tolerância a Falhas
    // =========================================================================

    /**
     * Período de envio regular de Heartbeat pelas réplicas ao líder.
     * Predefinição: 2 000 ms (2 segundos).
     * Onde é utilizado: ReplicaService.iniciarHeartbeats().
     */
    public static final long PERIODO_HB_MS = Long.getLong("tpf.periodo", 2000L);

    /**
     * Intervalo sem heartbeats após o qual uma réplica transita para o estado SUSPEITA (3 heartbeats falhados).
     * Predefinição: 6 000 ms (6 segundos).
     * Onde é utilizado: HeartbeatMonitor.avaliar().
     */
    public static final long SUSPEITA_MS = Long.getLong("tpf.suspeita", 6000L);

    /**
     * Intervalo sem heartbeats após o qual uma réplica é dada como MORTA (5 heartbeats falhados).
     * Predefinição: 10 000 ms (10 segundos).
     * Onde é utilizado: HeartbeatMonitor.avaliar() e exclusão em ReplicaRegistry.reserveAndChoose().
     */
    public static final long MORTA_MS = Long.getLong("tpf.morta", 10000L);

    /**
     * Tempo de vida máximo de um token efémero gerado via /replica-token.
     * Predefinição: 600 000 ms (10 minutos).
     * Onde é utilizado: PubSubHub.processarOla() e PubSubHub.processarProva().
     */
    public static final long TOKEN_VALIDADE_MS = Long.getLong("tpf.tokenValidade", 600000L);

    /**
     * Tempo limite de espera pela confirmação ACK de um STORE enviado a uma réplica.
     * Predefinição: 4 000 ms (4 segundos).
     * Onde é utilizado: LeaderBot.distribuirChunkReplicas() para agendamento de rondas seguintes.
     */
    public static final long ACK_TIMEOUT_MS = Long.getLong("tpf.ackTimeout", 4000L);

    /**
     * Número máximo de tentativas de colocação (rondas) para tentar obter R cópias de um chunk.
     * Predefinição: 3 rondas.
     * Onde é utilizado: LeaderBot.agendarNovaRonda().
     */
    public static final int MAX_RONDAS = Integer.getInteger("tpf.maxRondas", 3);

    /**
     * Intervalo de reconconciliação em segundo plano (preparação para o Sprint 4).
     */
    public static final long RECONCILIACAO_MS = Long.getLong("tpf.reconciliacao", 5000L);

    /**
     * Intervalo de publicação de manifesto periódico (preparação para o Sprint 4).
     */
    public static final long MANIFESTO_MS = Long.getLong("tpf.manifesto", 15000L);

    // =========================================================================
    // 4. Rede WebSocket e Backoff Exponencial
    // =========================================================================

    /**
     * Porto TCP do servidor WebSocket embutido no processo do Líder.
     * Predefinição: 8765.
     * Onde é utilizado: PubSubHub (líder) e ReplicaService (réplicas).
     */
    public static final int PORTO = Integer.getInteger("tpf.porto", 8765);

    /**
     * Tempo mínimo de espera inicial para religação após desconexão (Backoff).
     * Predefinição: 1 000 ms (1 segundo).
     * Onde é utilizado: ReplicaService.agendarReligacao().
     */
    public static final long BACKOFF_MIN_MS = Long.getLong("tpf.backoffMin", 1000L);

    /**
     * Tempo máximo de espera para religação (teto do Backoff Exponencial).
     * Predefinição: 30 000 ms (30 segundos).
     * Onde é utilizado: ReplicaService.agendarReligacao().
     */
    public static final long BACKOFF_MAX_MS = Long.getLong("tpf.backoffMax", 30000L);

    // =========================================================================
    // 5. Nomes de Tópicos do Protocolo Pub/Sub
    // =========================================================================

    /** Tópico de destino para mensagens enviadas pelas réplicas ao líder (HB, INVENTARIO, ACK). */
    public static final String TOPICO_LIDER = "tpf-lider";

    /** Tópico de controlo para difusão do líder para todas as réplicas autenticadas. */
    public static final String TOPICO_CTL = "tpf-ctl";

    /**
     * Devolve o nome do tópico privado de uma réplica individual: "tpf-r-<id>".
     * Onde é utilizado: LeaderBot para envio de STORE e ReplicaService para subscrição exclusiva.
     *
     * @param id Identificador da réplica (ex: "A", "replica1")
     * @return Tópico formatado "tpf-r-<id>"
     */
    public static String topicReplica(String id) {
        return "tpf-r-" + id;
    }

    /**
     * Calcula o número mínimo de ACKs necessários para um chunk atingir o estado DURAVEL:
     * Fórmula da especificação: max(W_MIN, floor(R / 2) + 1).
     * Exemplos:
     * - Para R = 3: max(2, 1 + 1) = 2 ACKs necessários.
     * - Para R = 5: max(2, 2 + 1) = 3 ACKs necessários.
     *
     * Onde é utilizado: Catalog.confirmar() e LeaderBot.onSlashCommandInteraction().
     *
     * @param r Fator de replicação atual
     * @return Número de confirmações mínimas necessárias
     */
    public static int quorumDuravel(int r) {
        int maioria = (r / 2) + 1;
        return Math.max(W_MIN, maioria);
    }

    private Wire() {}
}
