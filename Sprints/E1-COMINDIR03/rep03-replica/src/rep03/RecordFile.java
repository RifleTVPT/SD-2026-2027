package rep03;

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

    public synchronized long lastSeq(int n) throws IOException {
        SensorRecord r = last(n);
        return r != null ? r.getSeq() : 0;
    }

    public synchronized SensorRecord last(int n) throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        SensorRecord lastRecord = null;
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        int lineNum = 0;

        for (String line : lines) {
            lineNum++;
            if (!line.isBlank()) {
                try {
                    lastRecord = SensorRecord.fromLine(line, n);
                } catch (Exception e) {
                    throw new IOException("Ficheiro corrompido (" + path.getFileName()
                            + ") na linha " + lineNum + ": " + line, e);
                }
            }
        }
        return lastRecord;
    }

    public synchronized List<SensorRecord> range(long from, long to, int n) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            SensorRecord r = SensorRecord.fromLine(line, n);
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }
}