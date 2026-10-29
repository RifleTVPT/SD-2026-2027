package rep04;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = records != null ? records : new ArrayList<>();
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(";").append(port).append(";")
          .append(inc).append(";").append(hb).append(";")
          .append(lastSeq).append(";").append(records.size()).append("\n");
        for (SensorRecord r : records) {
            sb.append(r.toLine()).append("\n");
        }
        return sb.toString().trim();
    }

    public static Batch decode(String msg, int nParticoes) {
        String[] lines = msg.trim().split("\r?\n");
        if (lines.length == 0) throw new IllegalArgumentException("Mensagem de LOTE vazia");

        String[] header = lines[0].split(";");
        if (header.length != 7 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("Cabeçalho de LOTE malformado: " + lines[0]);
        }

        String address = header[1];
        int port = Integer.parseInt(header[2]);
        long inc = Long.parseLong(header[3]);
        long hb = Long.parseLong(header[4]);
        long lastSeq = Long.parseLong(header[5]);
        int nRegs = Integer.parseInt(header[6]);

        if (lines.length - 1 != nRegs) {
            throw new IllegalArgumentException("Contagem de linhas (" + (lines.length - 1) + ") diverge de n (" + nRegs + ")");
        }

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                records.add(SensorRecord.fromLine(lines[i], nParticoes));
            }
        }

        return new Batch(address, port, inc, hb, lastSeq, records);
    }

    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getInc() { return inc; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public List<SensorRecord> getRecords() { return records; }
}
