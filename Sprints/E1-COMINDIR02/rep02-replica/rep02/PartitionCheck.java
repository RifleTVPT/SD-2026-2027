package rep02;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class PartitionCheck {
    public static void main(String[] args) {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String pastaStr = args.length > 1 ? args[1] : ".";

        File dir = new File(pastaStr);
        Partitioner partitioner = new Partitioner(N);
        int totalRegistosSistema = 0;

        System.out.println("=== RELATÓRIO DE VERIFICAÇÃO (PartitionCheck) ===");

        for (int k = 0; k < N; k++) {
            System.out.println("\n--- PARTIÇÃO p" + k + " ---");
            File fileLider = new File(dir, "lider-p" + k + ".txt");

            List<SensorRecord> registosLider = carregaValidaFicheiro(fileLider, k, partitioner, true);
            int countLider = registosLider.size();
            totalRegistosSistema += countLider;

            Set<String> sensoresDistintos = new HashSet<>();
            for (SensorRecord r : registosLider) {
                sensoresDistintos.add(r.getSensorId());
            }

            System.out.println("Líder p" + k + ": " + countLider + " registos, " + sensoresDistintos.size() + " sensores distintos (" + sensoresDistintos + ")");

            int numReplicasEncontradas = 0;
            for (int rId = 1; rId <= 10; rId++) {
                File fileReplica = new File(dir, "replica-" + rId + "-p" + k + ".txt");
                if (fileReplica.exists()) {
                    numReplicasEncontradas++;
                    List<SensorRecord> registosReplica = carregaValidaFicheiro(fileReplica, k, partitioner, false);

                        boolean eIgual = (registosReplica.size() == countLider)
                            && ePrefixo(registosLider, registosReplica);
                        boolean ePrefixo = registosReplica.size() <= countLider
                            && ePrefixo(registosLider, registosReplica);
                        int faltam = Math.max(0, countLider - registosReplica.size());
                        boolean excedeLider = registosReplica.size() > countLider;

                    System.out.println("  Réplica " + rId + " (replica-" + rId + "-p" + k + ".txt): "
                            + registosReplica.size() + " registos | É igual? " + eIgual
                            + " | É prefixo? " + ePrefixo + " | Faltam: " + faltam
                            + " | Excede líder? " + excedeLider);
                }
            }
            System.out.println("Fator de replicação observado em p" + k + ": R = " + numReplicasEncontradas);
        }

        System.out.println("\n================================================");
        System.out.println("TOTAL DE REGISTOS NO SISTEMA: " + totalRegistosSistema);
        System.out.println("================================================\n");
    }

    private static List<SensorRecord> carregaValidaFicheiro(File file, int particaoEsperada, Partitioner partitioner, boolean eLider) {
        List<SensorRecord> list = new ArrayList<>();
        if (!file.exists()) return list;

        try {
            List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
            long expectedSeq = 1;

            for (String line : lines) {
                if (line.isBlank()) continue;
                SensorRecord r = SensorRecord.fromLine(line);

                if (eLider) {
                    int p = partitioner.of(r.getSensorId());
                    if (p != particaoEsperada) {
                        System.err.println("ERRO: Sensor " + r.getSensorId() + " no ficheiro " + file.getName() + " pertence a p" + p + " e não a p" + particaoEsperada);
                    }
                }

                if (r.getSeq() != expectedSeq) {
                    System.err.println("ERRO DE SEQUÊNCIA no ficheiro " + file.getName() + ": esperado " + expectedSeq + ", encontrado " + r.getSeq());
                }
                expectedSeq++;
                list.add(r);
            }
        } catch (Exception e) {
            System.err.println("Erro ao ler ficheiro " + file.getName() + ": " + e.getMessage());
        }
        return list;
    }

    private static boolean ePrefixo(List<SensorRecord> lider, List<SensorRecord> replica) {
        if (replica.size() > lider.size()) return false;
        for (int i = 0; i < replica.size(); i++) {
            if (replica.get(i).getSeq() != lider.get(i).getSeq()) {
                return false;
            }
        }
        return true;
    }
}