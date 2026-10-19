package rep01;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
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

    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        int lineNum = 0;
        for (String line : lines) {
            lineNum++;
            if (!line.isBlank()) {
                try {
                    last = SensorRecord.fromLine(line).getSeq();
                } catch (Exception e) {
                    throw new IOException("Ficheiro corrompido (" + path.getFileName()
                            + ") na linha " + lineNum + ": " + line, e);
                }
            }
        }
        return last;
    }
}