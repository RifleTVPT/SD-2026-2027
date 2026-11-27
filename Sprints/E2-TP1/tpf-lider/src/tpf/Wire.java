package tpf;

public final class Wire {
    // Tamanho máximo em bytes de um chunk aberto antes de forçar a selagem (predefinido: 64 KB)
    public static final long CHUNK_MAX_BYTES = Long.parseLong(System.getProperty("tpf.chunkMax", "65536"));

    // Múltiplo de alinhamento para o padding dos blocos em bytes (predefinido: 4 KB)
    public static final int PAD_BYTES = Integer.parseInt(System.getProperty("tpf.pad", "4096"));

    // Tempo limite em milissegundos para fechar o chunk aberto por temporizador (predefinido: 5 segundos)
    public static final long SELAGEM_MS = Long.parseLong(System.getProperty("tpf.selagem", "5000"));

    // Constantes adicionais para os próximos sprints
    public static final int R = Integer.getInteger("tpf.r", 3);
    public static final int W = Integer.getInteger("tpf.w", 2);
    public static final double ALFA = Double.parseDouble(System.getProperty("tpf.alfa", "1.0"));
    public static final long PERIODO_HB_MS = Long.getLong("tpf.periodo", 2000L);
    public static final long MORTA_MS = Long.getLong("tpf.morta", 30000L);
    public static final long RECONCILIACAO_MS = Long.getLong("tpf.reconciliacao", 5000L);
    public static final long ACK_TIMEOUT_MS = Long.getLong("tpf.ackTimeout", 4000L);
    public static final long MARGEM_BYTES = Long.getLong("tpf.margem", 65536L);
    public static final long MANIFESTO_MS = Long.getLong("tpf.manifesto", 15000L);
    public static final int PORTO = Integer.getInteger("tpf.porto", 8765);

    public static final String TOPICO_LIDER = "tpf-lider";
    public static final String TOPICO_CTL = "tpf-ctl";

    public static String topicReplica(String id) {
        return "tpf-r-" + id;
    }

    private Wire() {}
}