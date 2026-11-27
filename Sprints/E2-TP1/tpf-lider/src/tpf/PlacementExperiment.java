package tpf;

import java.util.*;

/**
 * Experiência Estatística 5 de Colocação Ponderada (Placement Benchmark).
 * 
 * PAPEL NO SISTEMA:
 * Valida empiricamente o comportamento matemático do algoritmo Rendezvous Hashing (Placement):
 * - Simula a colocação de 10 000 identificadores de chunk distintos em 4 réplicas com quotas diferentes:
 *   - Réplica A: 8 MB (40% do total de espaço)
 *   - Réplica B: 2 MB (10% do total de espaço)
 *   - Réplica C: 4 MB (20% do total de espaço)
 *   - Réplica D: 6 MB (30% do total de espaço)
 * - Avalia a primeira escolha (ranking #1) para três valores de alfa:
 *   * alfa = 0.0: Distribuição puramente uniforme (~25% para cada uma das 4 máquinas).
 *   * alfa = 1.0: Distribuição proporcional ao espaço (~40% para A, ~10% para B, ~20% para C, ~30% para D).
 *   * alfa = 2.0: Distribuição com forte viés para nós maiores (~53% para A, ~3.6% para B).
 * 
 * MODELO DE THREADS:
 * Executado diretamente na Thread Main via consola:
 * java -cp out tpf.PlacementExperiment
 */
public class PlacementExperiment {

    public static void main(String[] args) {
        int totalChunks = 10_000;
        long tamanhoChunk = 4096L;
        long mb = 1024L * 1024L;

        // Configuração das 4 réplicas laboratoriais da experiência
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

            // Simula 10 000 chunks com identificadores determinísticos
            for (int i = 0; i < totalChunks; i++) {
                String chunkId = String.format("%064x", i + 1);
                List<String> escolhidas = p.choose(chunkId, tamanhoChunk, 1, replicas, Collections.emptySet());
                if (!escolhidas.isEmpty()) {
                    contagem.merge(escolhidas.get(0), 1, Integer::sum);// se a lista escolhida não estiver vazia, adiciona 1 ao contador da primeira réplica escolhida
                }
            }

            System.out.println("=== Experiência 5: alfa = " + alfa + " (10 000 chunks) ===");
            // mostra os resultados de cada réplica em percentagem, quantos chunks foram colocados em cada réplica
            // neste caso como so ganha 1 replica, verifica-se bem, a diferença dos 3 tipos de alfas
            for (Map.Entry<String, Integer> e : contagem.entrySet()) {
                double pct = (e.getValue() * 100.0) / totalChunks;
                System.out.printf("  %-10s -> %5d chunks (%.2f%%)%n", e.getKey(), e.getValue(), pct);
            }
            System.out.println();
        }
    }
}
