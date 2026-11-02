package rep04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    public static final double LIMIAR_ALARME = 28.0;

    public static final long PERIODO_HB_MS = Long.parseLong(System.getProperty("rep04.periodo", "5000"));
    public static final int FATOR_TIMEOUT = Integer.parseInt(System.getProperty("rep04.fator", "3"));
    public static final long VERIFICACAO_MS = 1000;

    private Wire() { }

    public static String topicIn(int k)   { return "rep04-in-p" + k; }
    public static String topicData(int k) { return "rep04-p" + k; }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector.toString();
    }

    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        return m.substring(0, i);
    }

    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}
