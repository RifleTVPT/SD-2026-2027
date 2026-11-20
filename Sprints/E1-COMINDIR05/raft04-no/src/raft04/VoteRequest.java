package raft04;

public class VoteRequest {
    private final long term;
    private final int candidate;
    private final long lastSeq;
    private final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !"PEDIDO_VOTO".equals(f[0])) {
            throw new IllegalArgumentException("Formato PEDIDO_VOTO inválido: " + msg);
        }
        return new VoteRequest(Long.parseLong(f[1]), Integer.parseInt(f[2]),
                               Long.parseLong(f[3]), Long.parseLong(f[4]));
    }

    public long getTerm() { return term; }
    public int getCandidate() { return candidate; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
}
