package raft04;

public class VoteReply {
    private final long term;
    private final int voter;
    private final int candidate;
    private final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !"VOTO".equals(f[0])) {
            throw new IllegalArgumentException("Formato VOTO inválido: " + msg);
        }
        return new VoteReply(Long.parseLong(f[1]), Integer.parseInt(f[2]),
                             Integer.parseInt(f[3]), "S".equalsIgnoreCase(f[4]));
    }

    public long getTerm() { return term; }
    public int getVoter() { return voter; }
    public int getCandidate() { return candidate; }
    public boolean isGranted() { return granted; }
}
