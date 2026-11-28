package tpf;

import java.util.*;

/**
 * Experiência 5 do Sprint 3:
 * Mede a distribuição da primeira escolha de Placement.choose sobre 10 000 chunkIds
 * para 4 réplicas com quotas de 8, 2, 4 e 6 MB e alfa = 0, 1 e 2.
 */
public class PlacementExperiment {

    public static void main(String[] args) {
        int totalChunks = 10_000;
        long tamanhoChunk = 4096L;
        long mb = 1024L * 1024L;

        List<Placement.Candidate> replicas = List.of(
                new Placement.Candidate("A (8 MB)", 8 * mb),
                new Placement.Candidate("B (2 MB)", 2 * mb),
                new Placement.Candidate("C (4 MB)", 4 * mb),
                new Placement.Candidate("D (6 MB)", 6 * mb)
        );

        for (double alfa : new double[]{0.0, 1.0, 2.0}) {
            Placement p = new Placement(alfa, Wire.MARGEM_BYTES);
            Map<String, Integer> contagem = new LinkedHashMap<>();
            for (Placement.Candidate c : replicas) contagem.put(c.id, 0);

            for (int i = 0; i < totalChunks; i++) {
                String chunkId = String.format("%064x", i + 1);
                List<String> escolhidas = p.choose(chunkId, tamanhoChunk, 1, replicas, Collections.emptySet());
                if (!escolhidas.isEmpty()) {
                    contagem.merge(escolhidas.get(0), 1, Integer::sum);
                }
            }

            System.out.println("=== Experiência 5: alfa = " + alfa + " (10 000 chunks) ===");
            for (Map.Entry<String, Integer> e : contagem.entrySet()) {
                double pct = (e.getValue() * 100.0) / totalChunks;
                System.out.printf("  %-10s -> %5d chunks (%.2f%%)%n", e.getKey(), e.getValue(), pct);
            }
            System.out.println();
        }
    }
}