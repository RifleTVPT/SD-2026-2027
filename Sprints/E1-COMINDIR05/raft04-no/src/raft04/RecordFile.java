package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized void rewriteAll(List<SensorRecord> list) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        StringBuilder sb = new StringBuilder();
        for (SensorRecord r : list) {
            sb.append(r.toLine()).append(System.lineSeparator());
        }
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public synchronized SensorRecord last(int n) throws IOException {
        if (!Files.exists(path)) return null;
        SensorRecord lastRecord = null;
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        for (String line : lines) {
            if (!line.isBlank()) {
                lastRecord = SensorRecord.fromLine(line, n);
            }
        }
        return lastRecord;
    }

    public synchronized List<SensorRecord> readAll(int n) throws IOException {
        List<SensorRecord> list = new ArrayList<>();
        if (!Files.exists(path)) return list;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                list.add(SensorRecord.fromLine(line, n));
            }
        }
        return list;
    }

    public synchronized List<SensorRecord> range(long from, long to, int n) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) return out;
        for (SensorRecord r : readAll(n)) {
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }
}
