package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class Wire {
    public static final int GRUPO = 3;
    public static final long PERIODO_HB_MS = Long.parseLong(System.getProperty("raft04.periodo", "2000"));
    public static final int FATOR_TIMEOUT = Integer.parseInt(System.getProperty("raft04.fator", "3"));
    public static final long VERIFICACAO_MS = 200;

    public static final long ELEICAO_MIN_MS = Long.parseLong(System.getProperty("raft04.eleicaoMin", String.valueOf(3 * PERIODO_HB_MS)));
    public static final long ELEICAO_MAX_MS = Long.parseLong(System.getProperty("raft04.eleicaoMax", String.valueOf(6 * PERIODO_HB_MS)));

    public static final int PORTO_BASE = 7000;
    public static final double LIMIAR_ALARME = 28.0;

    private Wire() { }

    public static String topicIn(int k)       { return "raft04-in-p" + k; }
    public static String topicData(int k)     { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }

    public static int nodePort(int k, int i) {
        return PORTO_BASE + (k * 10) + i;
    }

    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector.toString();
    }

    public static String type(String msg) {
        String m = msg.trim();
        int idx = m.indexOf(';');
        if (idx <= 0) throw new IllegalArgumentException("Sem tipo: " + msg);
        return m.substring(0, idx);
    }

    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}
