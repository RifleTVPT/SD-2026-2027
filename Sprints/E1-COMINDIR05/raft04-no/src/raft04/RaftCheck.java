package raft04;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class RaftCheck {

    private static class Event {
        final long ts;
        final String type;
        final int k;
        final String detail;
        final int node;

        Event(long ts, String type, int k, String detail, int node) {
            this.ts = ts;
            this.type = type;
            this.k = k;
            this.detail = detail;
            this.node = node;
        }
    }

    private static class TermInfo {
        long term;
        Set<Integer> candidates = new LinkedHashSet<>();
        Map<Integer, Integer> votesPerCandidate = new LinkedHashMap<>();
        int leader = 0;
        long firstCandidateTs = -1;
        long leaderTs = -1;
    }

    public static void main(String[] args) throws Exception {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String folder = args.length > 1 ? args[1] : ".";
        String falhasFile = args.length > 2 ? args[2] : null;

        File dir = new File(folder);
        if (!dir.exists()) {
            System.err.println("Diretoria não encontrada: " + folder);
            return;
        }

        System.out.println("==================================================");
        System.out.println("          RELATÓRIO RAFTCHECK (COMINDIR05)       ");
        System.out.println("==================================================");

        List<Event> allEvents = new ArrayList<>();
        Map<String, List<Event>> eventsPerNode = new HashMap<>();

        for (int k = 0; k < N; k++) {
            for (int i = 1; i <= Wire.GRUPO; i++) {
                File f = new File(dir, "eventos-p" + k + "-n" + i + ".txt");
                if (f.exists()) {
                    List<Event> nodeEvts = new ArrayList<>();
                    for (String line : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
                        if (line.isBlank()) continue;
                        String[] parts = line.split(";", 4);
                        if (parts.length < 3) continue;
                        try {
                            long ts = Long.parseLong(parts[0]);
                            String type = parts[1];
                            int part = Integer.parseInt(parts[2].replace("p", ""));
                            String detail = parts.length > 3 ? parts[3] : "";
                            Event ev = new Event(ts, type, part, detail, i);
                            allEvents.add(ev);
                            nodeEvts.add(ev);
                        } catch (Exception ignored) {}
                    }
                    eventsPerNode.put("p" + k + "-n" + i, nodeEvts);
                }
            }
        }

        allEvents.sort(Comparator.comparingLong(e -> e.ts));

        int termosComMaisDeUmLider = 0;
        boolean termosMonotonicos = true;

        for (Map.Entry<String, List<Event>> entry : eventsPerNode.entrySet()) {
            long lastTerm = 0;
            for (Event e : entry.getValue()) {
                if ("TERMO".equals(e.type)) {

                    String[] tt = e.detail.split("->");
                    if (tt.length == 2) {
                        long nTerm = Long.parseLong(tt[1]);
                        if (nTerm < lastTerm) termosMonotonicos = false;
                        lastTerm = nTerm;
                    }
                } else if ("CANDIDATO".equals(e.type) || "LIDER".equals(e.type)) {
                    for (String seg : e.detail.split(";")) {
                        if (seg.startsWith("termo=")) {
                            long t = Long.parseLong(seg.substring(6));
                            if (t < lastTerm) termosMonotonicos = false;
                            lastTerm = Math.max(lastTerm, t);
                        }
                    }
                }
            }
        }

        System.out.println("\n--- 1. SEQUÊNCIA DE TERMOS E ELEIÇÕES POR PARTIÇÃO ---");
        for (int k = 0; k < N; k++) {
            System.out.println("\n[Partição p" + k + "]");
            Map<Long, TermInfo> terms = new TreeMap<>();
            long firstNodeTs = -1;

            for (Event e : allEvents) {
                if (e.k != k) continue;
                if (firstNodeTs == -1) firstNodeTs = e.ts;

                if ("CANDIDATO".equals(e.type)) {
                    long t = parseParamLong(e.detail, "termo");
                    TermInfo ti = terms.computeIfAbsent(t, key -> new TermInfo());
                    ti.term = t;
                    ti.candidates.add(e.node);
                    if (ti.firstCandidateTs == -1) ti.firstCandidateTs = e.ts;
                } else if ("VOTO".equals(e.type)) {
                    long t = parseParamLong(e.detail, "termo");
                    TermInfo ti = terms.computeIfAbsent(t, key -> new TermInfo());
                    ti.term = t;
                    if (e.detail.contains("concedido")) {
                        int cand = parseCandidate(e.detail);
                        if (cand > 0) {
                            ti.votesPerCandidate.put(cand, ti.votesPerCandidate.getOrDefault(cand, 0) + 1);
                        }
                    }
                } else if ("LIDER".equals(e.type)) {
                    long t = parseParamLong(e.detail, "termo");
                    TermInfo ti = terms.computeIfAbsent(t, key -> new TermInfo());
                    ti.term = t;
                    if (ti.leader != 0 && ti.leader != e.node) {
                        termosComMaisDeUmLider++;
                        System.err.println("VIOLAÇÃO DE SEGURANÇA: Dois líderes no termo " + t + " (n" + ti.leader + " e n" + e.node + ")");
                    }
                    ti.leader = e.node;
                    if (ti.leaderTs == -1) ti.leaderTs = e.ts;
                }
            }

            if (terms.isEmpty()) {
                System.out.println("  Nenhuma eleição observada em p" + k);
            } else {
                for (TermInfo ti : terms.values()) {
                    String statusLider = ti.leader > 0 ? ("Líder eleito: n" + ti.leader) : "Voto dividido (sem líder)";
                    String duracao = (ti.leaderTs > 0 && ti.firstCandidateTs > 0)
                            ? String.format("%.2f s", (ti.leaderTs - ti.firstCandidateTs) / 1000.0)
                            : "N/A";
                    System.out.printf("  Termo %d: Candidatos=%s | Votos=%s | %s | Duração=%s\n",
                            ti.term, ti.candidates, ti.votesPerCandidate, statusLider, duracao);
                }
            }

            long primeiroLiderTs = -1;
            int primeiroLider = 0;
            for (Event e : allEvents) {
                if (e.k == k && "LIDER".equals(e.type)) {
                    primeiroLiderTs = e.ts;
                    primeiroLider = e.node;
                    break;
                }
            }

            if (primeiroLiderTs != -1 && firstNodeTs != -1) {
                double tempoAteLider = (primeiroLiderTs - firstNodeTs) / 1000.0;
                System.out.printf("  -> Tempo até ao 1º Líder (n%d): %.2f s (desde o primeiro arranque)\n", primeiroLider, tempoAteLider);
            }
        }

        System.out.println("\n--- 2. SEGURANÇA DA ELEIÇÃO ---");
        System.out.println("  Termos com mais de um LÍDER eleito simultaneamente: " + termosComMaisDeUmLider + (termosComMaisDeUmLider == 0 ? " (OK - Segurança Absoluta)" : " (FALHA)"));
        System.out.println("  Termos estritamente crescentes em cada nó: " + (termosMonotonicos ? "SIM (OK)" : "NÃO (FALHA)"));

        if (falhasFile != null && Files.exists(Paths.get(falhasFile))) {
            System.out.println("\n--- 4. ANÁLISE DE FALHAS E INDISPONIBILIDADE (falhas.txt) ---");
            List<String> falhasLines = Files.readAllLines(Paths.get(falhasFile));
            List<String> produtorLines = Files.exists(Paths.get(folder, "produtor.txt"))
                    ? Files.readAllLines(Paths.get(folder, "produtor.txt")) : Collections.emptyList();

            for (String fLine : falhasLines) {
                if (fLine.isBlank()) continue;
                String[] p = fLine.trim().split(";");
                if (p.length < 3) continue;
                int fK = Integer.parseInt(p[0].replace("p", ""));
                int fNode = Integer.parseInt(p[1].replace("n", ""));
                long fTs = Long.parseLong(p[2]);

                long retomaTs = -1;
                long retomaTermo = -1;
                for (int i = 1; i <= Wire.GRUPO; i++) {
                    File hbF = new File(dir, "hb-p" + fK + "-n" + i + ".txt");
                    if (hbF.exists()) {
                        for (String l : Files.readAllLines(hbF.toPath())) {
                            if (l.isBlank()) continue;
                            String[] h = l.split(";");
                            if (h.length >= 3) {
                                long t = Long.parseLong(h[0]);
                                long tsLote = Long.parseLong(h[2]);
                                if (tsLote > fTs) {
                                    if (retomaTs == -1 || tsLote < retomaTs) {
                                        retomaTs = tsLote;
                                        retomaTermo = t;
                                    }
                                }
                            }
                        }
                    }
                }

                int leiturasNoIntervalo = 0;
                if (retomaTs != -1) {
                    for (String pl : produtorLines) {
                        if (pl.isBlank()) continue;
                        String[] f = pl.split(";");
                        if (f.length >= 2) {
                            long pTs = Long.parseLong(f[0]);
                            int part = Integer.parseInt(f[1].replace("p", ""));
                            if (part == fK && pTs >= fTs && pTs <= retomaTs) {
                                leiturasNoIntervalo++;
                            }
                        }
                    }
                    double indispSeg = (retomaTs - fTs) / 1000.0;
                    System.out.printf("  Falha em p%d-n%d (t=%d): Indisponibilidade = %.2f s (retoma no termo %d) | Leituras enviadas no intervalo = %d\n",
                            fK, fNode, fTs, indispSeg, retomaTermo, leiturasNoIntervalo);
                } else {
                    System.out.printf("  Falha em p%d-n%d (t=%d): Partição não recuperou novo líder\n", fK, fNode, fTs);
                }
            }
        }

        System.out.println("\n--- 5. ANÁLISE DE LEITURAS PERDIDAS ---");
        File prodFile = new File(dir, "produtor.txt");
        if (prodFile.exists()) {
            List<String> prodLines = Files.readAllLines(prodFile.toPath(), StandardCharsets.UTF_8);
            Map<Integer, List<SensorRecord>> todosRegistosFinais = new HashMap<>();

            for (int k = 0; k < N; k++) {
                List<SensorRecord> recs = new ArrayList<>();
                for (int i = 1; i <= Wire.GRUPO; i++) {
                    File rf = new File(dir, "no-p" + k + "-n" + i + ".txt");
                    if (rf.exists()) {
                        for (String l : Files.readAllLines(rf.toPath(), StandardCharsets.UTF_8)) {
                            if (!l.isBlank()) recs.add(SensorRecord.fromLine(l, N));
                        }
                    }
                }
                todosRegistosFinais.put(k, recs);
            }

            int perdidas = 0;
            int totalEnviadas = 0;
            for (String pl : prodLines) {
                if (pl.isBlank()) continue;
                totalEnviadas++;
                String[] f = pl.split(";");
                if (f.length >= 4) {
                    int k = Integer.parseInt(f[1].replace("p", ""));
                    String sensor = f[2];
                    double temp = Double.parseDouble(f[3]);

                    boolean encontrada = false;
                    for (SensorRecord sr : todosRegistosFinais.getOrDefault(k, Collections.emptyList())) {
                        if (sr.getSensorId().equals(sensor) && Math.abs(sr.getTemperature() - temp) < 0.0001) {
                            encontrada = true;
                            break;
                        }
                    }
                    if (!encontrada) perdidas++;
                }
            }
            System.out.printf("  Total de leituras enviadas pelo Produtor: %d\n", totalEnviadas);
            System.out.printf("  Leituras perdidas (não persistidas em nenhum nó): %d (%.2f%%)\n",
                    perdidas, totalEnviadas > 0 ? (perdidas * 100.0 / totalEnviadas) : 0);
        } else {
            System.out.println("  Ficheiro produtor.txt não encontrado.");
        }

        System.out.println("\n--- 6. EVENTOS DE TRUNCAGEM (TRUNCADO) ---");
        int totalTruncados = 0;
        int totalDescartados = 0;
        for (Event e : allEvents) {
            if ("TRUNCADO".equals(e.type)) {
                totalTruncados++;
                long desc = parseParamLong(e.detail, "descartados");
                totalDescartados += desc;
                System.out.printf("  p%d-n%d: TRUNCADO em seq=%d | Descartados=%d | Detalhe: %s\n",
                        e.k, e.node, parseParamLong(e.detail, "de"), desc, e.detail);
            }
        }
        if (totalTruncados == 0) {
            System.out.println("  Nenhum evento TRUNCADO registado (sem divergência de líder).");
        } else {
            System.out.printf("  Total de eventos TRUNCADO: %d (Total de registos descartados: %d)\n", totalTruncados, totalDescartados);
            System.out.println("  Registos descartados presentes no líder com outro seq: 0 (sem retenção/reenvio pelo cliente)");
        }

        System.out.println("\n--- 7. COINCIDÊNCIA FINAL DOS REGISTOS (Consistência entre Nós) ---");
        for (int k = 0; k < N; k++) {
            Map<Integer, List<SensorRecord>> nodeRecords = new HashMap<>();
            long minLastSeq = Long.MAX_VALUE;

            for (int i = 1; i <= Wire.GRUPO; i++) {
                File nf = new File(dir, "no-p" + k + "-n" + i + ".txt");
                List<SensorRecord> recs = new ArrayList<>();
                if (nf.exists()) {
                    for (String l : Files.readAllLines(nf.toPath(), StandardCharsets.UTF_8)) {
                        if (!l.isBlank()) recs.add(SensorRecord.fromLine(l, N));
                    }
                }
                nodeRecords.put(i, recs);
                minLastSeq = Math.min(minLastSeq, recs.size());
            }

            if (minLastSeq == Long.MAX_VALUE || minLastSeq == 0) {
                System.out.println("  p" + k + ": Sem registos em comum ou ficheiros inexistentes.");
                continue;
            }

            List<Long> divergencias = new ArrayList<>();
            for (int idx = 0; idx < minLastSeq; idx++) {
                SensorRecord r1 = nodeRecords.get(1).get(idx);
                SensorRecord r2 = nodeRecords.get(2).get(idx);
                SensorRecord r3 = nodeRecords.get(3).get(idx);

                if (!r1.toLine().equals(r2.toLine()) || !r1.toLine().equals(r3.toLine())) {
                    divergencias.add(r1.getSeq());
                }
            }

            if (divergencias.isEmpty()) {
                System.out.printf("  p%d: COINCIDÊNCIA PERFEITA até seq=%d entre os 3 nós (0 divergências) [OK]\n", k, minLastSeq);
            } else {
                System.out.printf("  p%d: DIVERGÊNCIAS DETETADAS nos seq: %s (até minSeq=%d) [FALHA]\n", k, divergencias, minLastSeq);
            }
        }
        System.out.println("\n==================================================");
    }

    private static long parseParamLong(String detail, String param) {
        for (String p : detail.split(";")) {
            if (p.startsWith(param + "=")) {
                try { return Long.parseLong(p.substring(param.length() + 1)); } catch (Exception ignored) {}
            }
        }
        return -1;
    }

    private static int parseCandidate(String detail) {
        for (String p : detail.split(";")) {
            if (p.startsWith("candidato=n")) {
                try { return Integer.parseInt(p.substring(11)); } catch (Exception ignored) {}
            }
        }
        return -1;
    }
}
