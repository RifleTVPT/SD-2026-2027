package raft04;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class CausalCheck {

    private static class RecordEntry {
        final int k;
        final SensorRecord record;

        RecordEntry(int k, SensorRecord record) {
            this.k = k;
            this.record = record;
        }
    }

    public static void main(String[] args) {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String pastaStr = args.length > 1 ? args[1] : ".";

        File dir = new File(pastaStr);
        List<RecordEntry> todosRegistos = new ArrayList<>();

        System.out.println("=== RELATÓRIO CAUSAL (CausalCheck - COMINDIR05) ===");

        for (int k = 0; k < N; k++) {
            File escolhido = null;
            long maxSeq = -1;

            for (int i = 1; i <= Wire.GRUPO; i++) {
                File f = new File(dir, "no-p" + k + "-n" + i + ".txt");
                if (f.exists()) {
                    try {
                        RecordFile rf = new RecordFile(f.getPath());
                        SensorRecord last = rf.last(N);
                        long seq = last != null ? last.getSeq() : 0;
                        if (seq >= maxSeq) {
                            maxSeq = seq;
                            escolhido = f;
                        }
                    } catch (Exception ignored) {}
                }
            }

            if (escolhido == null || !escolhido.exists()) {
                System.out.println("Aviso: Nenhum ficheiro encontrado para a partição p" + k);
                continue;
            }

            try {
                List<String> lines = Files.readAllLines(escolhido.toPath(), StandardCharsets.UTF_8);
                System.out.println("Partição p" + k + ": lendo " + escolhido.getName() + " (" + lines.size() + " registos)");
                for (String line : lines) {
                    if (line.isBlank()) continue;
                    SensorRecord r = SensorRecord.fromLine(line, N);

                    if (r.getVector().get(k) != r.getSeq()) {
                        System.err.println("ERRO: V[" + k + "] != seq na partição p" + k + " (V[" + k + "]=" + r.getVector().get(k) + ", seq=" + r.getSeq() + "): " + line);
                    }
                    todosRegistos.add(new RecordEntry(k, r));
                }
            } catch (Exception e) {
                System.err.println("Erro ao ler " + escolhido.getName() + ": " + e.getMessage());
            }
        }

        long totalPares = 0, antesCount = 0, depoisCount = 0, concCount = 0;
        long concComLDistinto = 0;
        boolean violacaoLamport = false;

        for (int i = 0; i < todosRegistos.size(); i++) {
            for (int j = i + 1; j < todosRegistos.size(); j++) {
                RecordEntry e1 = todosRegistos.get(i);
                RecordEntry e2 = todosRegistos.get(j);

                if (e1.k == e2.k) continue;

                totalPares++;
                VectorClock.Order rel = e1.record.getVector().compare(e2.record.getVector());

                if (rel == VectorClock.Order.BEFORE) {
                    antesCount++;
                    if (e1.record.getLamport() >= e2.record.getLamport()) violacaoLamport = true;
                } else if (rel == VectorClock.Order.AFTER) {
                    depoisCount++;
                    if (e1.record.getLamport() <= e2.record.getLamport()) violacaoLamport = true;
                } else {
                    concCount++;
                    if (e1.record.getLamport() != e2.record.getLamport()) {
                        concComLDistinto++;
                    }
                }
            }
        }

        System.out.println("Total de registos analisados: " + todosRegistos.size());
        System.out.println("Total de pares inter-partição: " + totalPares);
        System.out.println("  Pares Causalmente Relacionados (a -> b ou b -> a): " + (antesCount + depoisCount));
        System.out.println("  Pares Concorrentes (a || b): " + concCount);
        System.out.println("  Pares Concorrentes com L distinto: " + concComLDistinto
                + " (" + (concCount > 0 ? String.format("%.2f%%", (concComLDistinto * 100.0 / concCount)) : "0%") + ")");
        System.out.println("Condição do Relógio de Lamport violada? " + (violacaoLamport ? "SIM (ERRO)" : "NÃO (VÁLIDO)"));

        todosRegistos.sort((r1, r2) -> {
            int cmp = Long.compare(r1.record.getLamport(), r2.record.getLamport());
            if (cmp != 0) return cmp;
            return Integer.compare(r1.k, r2.k);
        });

        try {
            List<String> outLines = new ArrayList<>();
            for (RecordEntry re : todosRegistos) {
                outLines.add("p" + re.k + ";" + re.record.toLine());
            }
            Files.write(new File(dir, "vista-lamport.txt").toPath(), outLines, StandardCharsets.UTF_8);
            System.out.println("Ficheiro 'vista-lamport.txt' gerado com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao escrever vista-lamport.txt: " + e.getMessage());
        }
    }
}
