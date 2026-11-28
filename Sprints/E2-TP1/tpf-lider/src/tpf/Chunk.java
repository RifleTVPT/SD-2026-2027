package tpf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class Chunk {

    // Representa uma entrada de registo individual dentro de um chunk.
    public static class RecordEntry {
        public final long lsn;
        public final long instante;
        public final String autor;
        public final String canal;
        public final String texto; // Texto guardado em Base64

        public RecordEntry(long lsn, long instante, String autor, String canal, String texto) {
            this.lsn = lsn;
            this.instante = instante;
            this.autor = autor;
            this.canal = canal;
            this.texto = texto;
        }

        // Converte a entrada de registo numa linha de texto formatada com ponto e vírgula (;).
        public String toLine() {
            return lsn + ";" + instante + ";" + autor + ";" + canal + ";" + texto;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RecordEntry that)) return false;
            return lsn == that.lsn && instante == that.instante &&
                   Objects.equals(autor, that.autor) &&
                   Objects.equals(canal, that.canal) &&
                   Objects.equals(texto, that.texto);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lsn, instante, autor, canal, texto);
        }
    }

    // Serializa uma lista de registos e aplica padding até um múltiplo de PAD_BYTES.
    // O padding serve para ocultar o tamanho exato dos dados (modelo Zero-Knowledge).
    public static byte[] serialize(List<RecordEntry> records, int padBytes) {
        StringBuilder sb = new StringBuilder();
        for (RecordEntry r : records) {
            sb.append(r.toLine()).append("\n");
        }

        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);

        int remainder = content.length % padBytes;
        int targetLength = (remainder == 0 && content.length > 0)
                ? content.length
                : content.length + (padBytes - remainder);

        byte[] padded = new byte[targetLength];
        System.arraycopy(content, 0, padded, 0, content.length);
        return padded;
    }

    // Faz a operação inversa: reconstrói a lista de registos a partir dos bytes (ignorando o padding a 0x00).
    public static List<RecordEntry> deserialize(byte[] paddedData) {
        int validLen = paddedData.length;
        while (validLen > 0 && paddedData[validLen - 1] == 0) {
            validLen--;
        }

        String text = new String(paddedData, 0, validLen, StandardCharsets.UTF_8);
        List<RecordEntry> records = new ArrayList<>();
        if (text.isEmpty()) return records;

        String[] lines = text.split("\n");
        for (String line : lines) {
            if (line.isEmpty()) continue;

            String[] parts = line.split(";", 5);
            if (parts.length == 5) {
                try {
                    long lsn = Long.parseLong(parts[0]);
                    long instante = Long.parseLong(parts[1]);
                    String autor = parts[2];
                    String canal = parts[3];
                    String texto = parts[4];
                    records.add(new RecordEntry(lsn, instante, autor, canal, texto));
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return records;
    }
}