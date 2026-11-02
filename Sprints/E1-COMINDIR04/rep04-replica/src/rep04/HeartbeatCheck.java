package rep04;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class HeartbeatCheck {

    private static class HbEntry {
        final long inc;
        final long hb;
        final long instante;
        final long ultimoSeq;
        final int n;
        final int bytes;

        HbEntry(long inc, long hb, long instante, long ultimoSeq, int n, int bytes) {
            this.inc = inc;
            this.hb = hb;
            this.instante = instante;
            this.ultimoSeq = ultimoSeq;
            this.n = n;
            this.bytes = bytes;
        }
    }

    private static class MonitorEvent {
        final String participante;
        final long instante;
        final String evento;
        final int k;
        final String detalhe;

        MonitorEvent(String participante, long instante, String evento, int k, String detalhe) {
            this.participante = participante;
            this.instante = instante;
            this.evento = evento;
            this.k = k;
            this.detalhe = detalhe;
        }
    }

    private static class FalhaEntry {
        final int k;
        final long instante;

        FalhaEntry(int k, long instante) {
            this.k = k;
            this.instante = instante;
        }
    }

    public static void main(String[] args) {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String pastaStr = args.length > 1 ? args[1] : ".";
        String falhasFile = args.length > 2 ? args[2] : null;

        File dir = new File(pastaStr);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Diretoria não encontrada: " + pastaStr);
            return;
        }

        System.out.println("================================================================================");
        System.out.println("                 RELATÓRIO HEARTBEAT E DETEÇÃO DE FALHAS (COMINDIR04)           ");
        System.out.println("================================================================================\n");

        Map<Integer, List<HbEntry>> hbMap = new TreeMap<>();
        for (int k = 0; k < N; k++) {
            File f = new File(dir, "hb-p" + k + ".txt");
            List<HbEntry> list = new ArrayList<>();
            if (f.exists()) {
                try {
                    List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
                    for (String line : lines) {
                        if (line.isBlank()) continue;
                        String[] parts = line.trim().split(";");
                        if (parts.length >= 6) {
                            list.add(new HbEntry(
                                    Long.parseLong(parts[0]),
                                    Long.parseLong(parts[1]),
                                    Long.parseLong(parts[2]),
                                    Long.parseLong(parts[3]),
                                    Integer.parseInt(parts[4]),
                                    Integer.parseInt(parts[5])
                            ));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Erro ao ler " + f.getName() + ": " + e.getMessage());
                }
            }
            hbMap.put(k, list);
        }

        Map<Integer, Map<Long, Long>> liderRecordsInstante = new TreeMap<>();
        Map<Integer, List<SensorRecord>> liderRecords = new TreeMap<>();
        for (int k = 0; k < N; k++) {
            File f = new File(dir, "lider-p" + k + ".txt");
            Map<Long, Long> map = new HashMap<>();
            List<SensorRecord> recs = new ArrayList<>();
            if (f.exists()) {
                try {
                    List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
                    for (String line : lines) {
                        if (line.isBlank()) continue;
                        SensorRecord r = SensorRecord.fromLine(line, N);
                        long instMs = parseInstante(r.getTimestamp());
                        map.put(r.getSeq(), instMs);
                        recs.add(r);
                    }
                } catch (Exception e) {
                    System.err.println("Erro ao ler " + f.getName() + ": " + e.getMessage());
                }
            }
            liderRecordsInstante.put(k, map);
            liderRecords.put(k, recs);
        }

        List<MonitorEvent> monitorEvents = new ArrayList<>();
        File[] monitorFiles = dir.listFiles((d, name) -> name.startsWith("monitor-") && name.endsWith(".txt"));
        if (monitorFiles != null) {
            for (File mf : monitorFiles) {
                String name = mf.getName();
                String participante = name.substring("monitor-".length(), name.length() - ".txt".length());
                try {
                    List<String> lines = Files.readAllLines(mf.toPath(), StandardCharsets.UTF_8);
                    for (String line : lines) {
                        if (line.isBlank()) continue;
                        String[] parts = line.trim().split(";", 4);
                        if (parts.length >= 3) {
                            long inst = Long.parseLong(parts[0]);
                            String evento = parts[1];
                            int k = -1;
                            if (parts[2].startsWith("p")) {
                                try {
                                    k = Integer.parseInt(parts[2].substring(1));
                                } catch (Exception ignored) {}
                            }
                            String detalhe = parts.length >= 4 ? parts[3] : "";
                            monitorEvents.add(new MonitorEvent(participante, inst, evento, k, detalhe));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Erro ao ler " + mf.getName() + ": " + e.getMessage());
                }
            }
        }

        List<FalhaEntry> falhas = new ArrayList<>();
        if (falhasFile != null) {
            File ff = new File(falhasFile);
            if (!ff.exists()) ff = new File(dir, falhasFile);
            if (ff.exists()) {
                try {
                    List<String> lines = Files.readAllLines(ff.toPath(), StandardCharsets.UTF_8);
                    for (String line : lines) {
                        if (line.isBlank()) continue;
                        String[] parts = line.trim().split(";");
                        if (parts.length >= 2) {
                            int k = parts[0].startsWith("p") ? Integer.parseInt(parts[0].substring(1)) : Integer.parseInt(parts[0]);
                            long inst = Long.parseLong(parts[1]);
                            falhas.add(new FalhaEntry(k, inst));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Erro ao ler " + ff.getName() + ": " + e.getMessage());
                }
            }
        }

        System.out.println("1. ESTATÍSTICAS DE LOTES E HEARTBEATS POR PARTIÇÃO E INCARNAÇÃO");
        System.out.println("--------------------------------------------------------------------------------");
        for (int k = 0; k < N; k++) {
            List<HbEntry> entries = hbMap.get(k);
            if (entries == null || entries.isEmpty()) {
                System.out.printf("  Partição p%d: (sem dados de hb-p%d.txt)%n", k, k);
                continue;
            }

            Map<Long, List<HbEntry>> porInc = new LinkedHashMap<>();
            for (HbEntry h : entries) {
                porInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            }

            for (Map.Entry<Long, List<HbEntry>> entry : porInc.entrySet()) {
                long inc = entry.getKey();
                List<HbEntry> list = entry.getValue();

                int totalLotes = list.size();
                int lotesVazios = 0;
                int maxRegs = 0;
                long somaRegs = 0;

                List<Long> intervalos = new ArrayList<>();
                for (int i = 0; i < list.size(); i++) {
                    HbEntry cur = list.get(i);
                    if (cur.n == 0) lotesVazios++;
                    somaRegs += cur.n;
                    if (cur.n > maxRegs) maxRegs = cur.n;

                    if (i > 0) {
                        intervalos.add(cur.instante - list.get(i - 1).instante);
                    }
                }

                double propVazios = totalLotes > 0 ? (lotesVazios * 100.0 / totalLotes) : 0.0;
                double mediaRegs = totalLotes > 0 ? (somaRegs * 1.0 / totalLotes) : 0.0;

                double mediaIntervalo = 0.0;
                long maxIntervalo = 0;
                if (!intervalos.isEmpty()) {
                    long somaInt = 0;
                    for (long iv : intervalos) {
                        somaInt += iv;
                        if (iv > maxIntervalo) maxIntervalo = iv;
                    }
                    mediaIntervalo = somaInt * 1.0 / intervalos.size();
                }

                System.out.printf("  Partição p%d (inc=%d):%n", k, inc);
                System.out.printf("    Total de Lotes: %d | Lotes Vazios: %d (%.1f%%)%n", totalLotes, lotesVazios, propVazios);
                System.out.printf("    Registos por lote: Média = %.2f | Máximo = %d%n", mediaRegs, maxRegs);
                System.out.printf("    Intervalo entre heartbeats: Média = %.1f ms | Máximo = %d ms%n%n", mediaIntervalo, maxIntervalo);
            }
        }

        System.out.println("2. ANÁLISE DE SUSPEITAS POR PARTICIPANTE E PARTIÇÃO");
        System.out.println("--------------------------------------------------------------------------------");
        Map<String, List<MonitorEvent>> porParticipante = new TreeMap<>();
        for (MonitorEvent ev : monitorEvents) {
            porParticipante.computeIfAbsent(ev.participante, x -> new ArrayList<>()).add(ev);
        }

        int totalFalsasSuspeitas = 0;
        int totalFalhasReais = 0;

        for (Map.Entry<String, List<MonitorEvent>> pEntry : porParticipante.entrySet()) {
            String partName = pEntry.getKey();
            List<MonitorEvent> pEvents = pEntry.getValue();

            System.out.printf("  Participante: %s%n", partName);

            for (int i = 0; i < pEvents.size(); i++) {
                MonitorEvent ev = pEvents.get(i);
                if ("SUSPEITA".equals(ev.evento)) {

                    boolean falsaSuspeita = false;
                    for (int j = i + 1; j < pEvents.size(); j++) {
                        MonitorEvent next = pEvents.get(j);
                        if (next.k == ev.k && "RETOMA".equals(next.evento)) {
                            falsaSuspeita = true;
                            break;
                        }
                        if (next.k == ev.k && "REINICIO".equals(next.evento)) {
                            break;
                        }
                    }

                    if (falsaSuspeita) {
                        totalFalsasSuspeitas++;
                        System.out.printf("    [FALSA SUSPEITA] p%d em t=%d (%s)%n", ev.k, ev.instante, ev.detalhe);
                    } else {
                        totalFalhasReais++;
                        System.out.printf("    [FALHA REAL]     p%d em t=%d (%s)%n", ev.k, ev.instante, ev.detalhe);
                    }
                }
            }
        }
        System.out.printf("%n  Total de Falsas Suspeitas: %d | Falhas Reais Detetadas: %d%n%n", totalFalsasSuspeitas, totalFalhasReais);

        if (!falhas.isEmpty()) {
            System.out.println("3. TEMPO DE DETEÇÃO DE FALHAS REAIS (A PARTIR DE falhas.txt)");
            System.out.println("--------------------------------------------------------------------------------");
            for (FalhaEntry f : falhas) {
                System.out.printf("  Falha em p%d no instante %d:%n", f.k, f.instante);
                for (Map.Entry<String, List<MonitorEvent>> pEntry : porParticipante.entrySet()) {
                    String partName = pEntry.getKey();
                    long menorTempoMs = -1;
                    for (MonitorEvent me : pEntry.getValue()) {
                        if (me.k == f.k && "SUSPEITA".equals(me.evento) && me.instante >= f.instante) {
                            long diff = me.instante - f.instante;
                            if (menorTempoMs == -1 || diff < menorTempoMs) {
                                menorTempoMs = diff;
                            }
                        }
                    }
                    if (menorTempoMs >= 0) {
                        System.out.printf("    %s: %d ms (%.2f s)%n", partName, menorTempoMs, menorTempoMs / 1000.0);
                    } else {
                        System.out.printf("    %s: (não detetada)%n", partName);
                    }
                }
            }
            System.out.println();
        }

        System.out.println("4. LATÊNCIA DE LOTE (RECEBIDO - INSTANTE REGISTO)");
        System.out.println("--------------------------------------------------------------------------------");
        for (int k = 0; k < N; k++) {
            Map<Long, Long> liderMap = liderRecordsInstante.get(k);
            List<Long> latencias = new ArrayList<>();

            for (MonitorEvent me : monitorEvents) {
                if (me.k == k && "RECEBIDO".equals(me.evento) && me.participante.startsWith("replica")) {
                    String[] f = me.detalhe.split(";");
                    String seqsStr = "";
                    for (String part : f) {
                        if (part.startsWith("seqs=")) {
                            seqsStr = part.substring("seqs=".length());
                        }
                    }
                    if (!seqsStr.isBlank() && !"-".equals(seqsStr)) {
                        String[] range = seqsStr.split("\\.\\.");
                        if (range.length == 2) {
                            long de = Long.parseLong(range[0]);
                            long ate = Long.parseLong(range[1]);
                            for (long s = de; s <= ate; s++) {
                                Long tReg = liderMap.get(s);
                                if (tReg != null) {
                                    long lat = me.instante - tReg;
                                    if (lat >= 0 && lat < 60000) {
                                        latencias.add(lat);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (!latencias.isEmpty()) {
                long soma = 0;
                long max = 0;
                for (long l : latencias) {
                    soma += l;
                    if (l > max) max = l;
                }
                double media = soma * 1.0 / latencias.size();
                System.out.printf("  Partição p%d: Registos medidos = %d | Latência Média = %.1f ms | Máxima = %d ms%n",
                        k, latencias.size(), media, max);
            } else {
                System.out.printf("  Partição p%d: Sem dados suficientes para cálculo de latência de lote.%n", k);
            }
        }
        System.out.println();

        System.out.println("5. EVENTOS DE REDE (HB_PERDIDO E RECUPERACAO)");
        System.out.println("--------------------------------------------------------------------------------");
        long countHbPerdido = 0;
        long countRecuperacao = 0;
        for (MonitorEvent me : monitorEvents) {
            if ("HB_PERDIDO".equals(me.evento)) countHbPerdido++;
            if ("RECUPERACAO".equals(me.evento)) countRecuperacao++;
        }
        System.out.printf("  Total de eventos HB_PERDIDO: %d%n", countHbPerdido);
        System.out.printf("  Total de eventos RECUPERACAO: %d%n%n", countRecuperacao);

        System.out.println("6. EFICIÊNCIA DE CARGA ÚTIL (BYTES POR REGISTO)");
        System.out.println("--------------------------------------------------------------------------------");
        long totalBytesLoteComRegs = 0;
        long totalRegsEmLotes = 0;
        for (int k = 0; k < N; k++) {
            List<HbEntry> entries = hbMap.get(k);
            if (entries != null) {
                for (HbEntry h : entries) {
                    if (h.n > 0) {
                        totalBytesLoteComRegs += h.bytes;
                        totalRegsEmLotes += h.n;
                    }
                }
            }
        }

        double mediaBytesPorRegLote = totalRegsEmLotes > 0 ? (totalBytesLoteComRegs * 1.0 / totalRegsEmLotes) : 0.0;

        long totalBytesRegIndiv = 0;
        long totalRegsLider = 0;
        for (int k = 0; k < N; k++) {
            List<SensorRecord> recs = liderRecords.get(k);
            if (recs != null) {
                for (SensorRecord r : recs) {
                    String regStr = "REG;127.0.0.1;" + (7000 + k) + ";" + r.toLine();
                    totalBytesRegIndiv += regStr.getBytes(StandardCharsets.UTF_8).length;
                    totalRegsLider++;
                }
            }
        }
        double mediaBytesPorRegIndiv = totalRegsLider > 0 ? (totalBytesRegIndiv * 1.0 / totalRegsLider) : 88.0;

        System.out.printf("  Média em LOTE: %.1f bytes/registo (baseado em %d registos)%n", mediaBytesPorRegLote, totalRegsEmLotes);
        System.out.printf("  Média em REG individual (COMINDIR03): %.1f bytes/registo%n", mediaBytesPorRegIndiv);
        if (mediaBytesPorRegIndiv > 0 && mediaBytesPorRegLote > 0) {
            double poupanca = (1.0 - (mediaBytesPorRegLote / mediaBytesPorRegIndiv)) * 100.0;
            System.out.printf("  Poupança estimada de bytes: %.1f%%%n", poupanca);
        }
        System.out.println("================================================================================");
    }

    private static long parseInstante(String str) {
        if (str == null || str.isBlank()) return 0;
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            try {
                return LocalDateTime.parse(str)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
            } catch (Exception ex) {
                return 0;
            }
        }
    }
}
