package raft04;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private final long term;
    private final int leader;
    private final String address;
    private final int port;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;
    private final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = records != null ? records : new ArrayList<>();
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(term).append(";").append(leader).append(";")
          .append(address).append(";").append(port).append(";")
          .append(hb).append(";").append(lastSeq).append(";")
          .append(lastTerm).append(";").append(records.size()).append("\n");
        for (SensorRecord r : records) {
            sb.append(r.toLine()).append("\n");
        }
        return sb.toString().trim();
    }

    public static Batch decode(String msg, int nParticoes) {
        String[] lines = msg.trim().split("\r?\n");
        if (lines.length == 0) throw new IllegalArgumentException("LOTE vazio");

        String[] header = lines[0].split(";");
        if (header.length != 9 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido: " + lines[0]);
        }

        long term = Long.parseLong(header[1]);
        int leader = Integer.parseInt(header[2]);
        String address = header[3];
        int port = Integer.parseInt(header[4]);
        long hb = Long.parseLong(header[5]);
        long lastSeq = Long.parseLong(header[6]);
        long lastTerm = Long.parseLong(header[7]);
        int count = Integer.parseInt(header[8]);

        if (lines.length - 1 != count) {
            throw new IllegalArgumentException("Incompatibilidade no LOTE: " + (lines.length - 1) + " != " + count);
        }

        List<SensorRecord> recs = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                recs.add(SensorRecord.fromLine(lines[i], nParticoes));
            }
        }

        return new Batch(term, leader, address, port, hb, lastSeq, lastTerm, recs);
    }

    public long getTerm() { return term; }
    public int getLeader() { return leader; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
    public List<SensorRecord> getRecords() { return records; }
}
