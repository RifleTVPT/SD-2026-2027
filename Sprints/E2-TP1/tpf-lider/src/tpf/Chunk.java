package tpf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Representa a estrutura de dados interna e a serialização de Chunks em claro.
 * 
 * PAPEL NO SISTEMA:
 * - Agrupa registos individuais (RecordEntry) provenientes das mensagens do Discord (#arquivo)
 *   ou do comando /registar.
 * - Converte registos estruturados num array de bytes contíguo com padding nulo (0x00).
 * - O padding alinha o tamanho a múltiplos de PAD_BYTES (4 KB), garantindo o modelo
 *   ZERO-KNOWLEDGE: observadores externos e réplicas voluntárias não conseguem inferir
 *   o número exato nem o tamanho real dos registos armazenados dentro do bloco cifrado.
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - RecordEntry é uma classe imutável (campos final).
 * - Chunk contém apenas métodos utilitários estáticos (serialize / deserialize) sem estado partilhado.
 * - Totalmente thread-safe e reentrante: pode ser invocado concorrentemente por múltiplas
 *   threads sem qualquer contenção de trincos.
 * 
 * ONDE É UTILIZADA:
 * - LeaderBot.selarChunkAtual(): serializa a lista aberta de registos antes da cifra.
 * - ChunkCryptoTest: valida que dados com padding recuperam exatamente os mesmos registos originais.
 */
public final class Chunk {

    /**
     * Entrada individual de registo durável armazenada dentro de um chunk.
     */
    public static class RecordEntry {
        public final long lsn;          // Log Sequence Number (monotónico crescente)
        public final long instante;     // Timestamp Epoch em milissegundos
        public final String autor;      // Nome do utilizador Discord ou sistema
        public final String canal;      // Nome do canal (#arquivo ou slash-command)
        public final String texto;      // Texto do registo codificado em Base64

        /**
         * Cria uma nova entrada de registo imutável.
         */
        public RecordEntry(long lsn, long instante, String autor, String canal, String texto) {
            this.lsn = lsn;
            this.instante = instante;
            this.autor = autor;
            this.canal = canal;
            this.texto = texto;
        }

        /**
         * Converte a entrada numa linha delimitada por ponto e vírgula (;):
         * "lsn;instante;autor;canal;textoBase64"
         * 
         * Onde é utilizado: Chunk.serialize() e gravação do LeaderBot em registos.log.
         */
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

    /**
     * Serializa uma lista de registos para UTF-8 e aplica padding até um múltiplo exato de padBytes.
     * 
     * Onde é utilizado: LeaderBot.selarChunkAtual() imediatamente antes de invocar ChunkCrypto.seal().
     *
     * @param records Lista de registos a empacotar
     * @param padBytes Tamanho do bloco de alinhamento (normalmente Wire.PAD_BYTES = 4096)
     * @return Array de bytes com texto UTF-8 seguido de bytes 0x00 de preenchimento
     */
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

    /**
     * Reconstrói a lista original de registos a partir de um bloco decifrado com padding.
     * Descarta os bytes nulos (0x00) no final até encontrar o último caractere '\n' válido.
     * 
     * Onde é utilizado: ChunkCryptoTest e futura recuperação de registos no Sprint 4.
     *
     * @param paddedData Array de bytes decifrado contendo o padding
     * @return Lista reconstruída de RecordEntry
     */
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

    private Chunk() {}
}
