package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class PartitionReplica {

    private static class PartitionState {
        final int k;
        final RecordFile file;
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        final Set<Long> descartadosSet = new HashSet<>();
        long ultimoSeq;
        String lastHost;
        int lastPort = -1;
        HeartbeatMonitor monitor;

        PartitionState(int k, String replicaId, int n) throws IOException {
            this.k = k;
            this.file = new RecordFile("replica-" + replicaId + "-p" + k + ".txt");
            this.ultimoSeq = file.lastSeq(n);
        }
    }

    private static final Object logLock = new Object();

    private static void logMonitor(String replicaId, String linha) {
        synchronized (logLock) {
            try {
                Files.writeString(Paths.get("monitor-replica" + replicaId + ".txt"),
                        linha + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Erro ao escrever no monitor: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String[] particoesStr = args.length > 1 ? args[1].split(",") : new String[]{"0"};
        int N = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        String apiPort = args.length > 3 ? args[3] : "5001";
        String apiUrl = "http://127.0.0.1:" + apiPort + "/api/v0";

        double probPerda = 0.0;
        String probPerdaProp = System.getProperty("perda");
        if (probPerdaProp != null) probPerda = Double.parseDouble(probPerdaProp);

        long seqDescartar = -1;
        String descartarProp = System.getProperty("descartar");
        if (descartarProp != null) seqDescartar = Long.parseLong(descartarProp);

        final double pPerda = probPerda;
        final long sDescartar = seqDescartar;

        IpfsPubSub ipfs = new IpfsPubSub(apiUrl);
        Random rand = new Random();
        List<HeartbeatMonitor> monitores = new CopyOnWriteArrayList<>();

        for (String pStr : particoesStr) {
            int k = Integer.parseInt(pStr.trim());

            new Thread(() -> {
                PartitionState state;
                try {
                    state = new PartitionState(k, id, N);
                } catch (IOException e) {
                    System.out.println("Erro ao criar estado da p" + k + ": " + e.getMessage());
                    return;
                }

                state.monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, evento -> {
                    String linha = System.currentTimeMillis() + ";" + evento;
                    System.out.println(linha);
                    logMonitor(id, linha);
                });
                monitores.add(state.monitor);

                System.out.println("Réplica " + id + " a ouvir p" + k + " | seq inicial: " + state.ultimoSeq);

                try {
                    ipfs.subscribe(Wire.topicData(k), msg -> {
                        Batch batch;
                        try {
                            batch = Batch.decode(msg, N);
                        } catch (Exception e) {
                            return;
                        }

                        synchronized (state) {
                            state.lastHost = batch.getAddress();
                            state.lastPort = batch.getPort();

                            state.monitor.onHeartbeat(batch.getInc(), batch.getHb());

                            List<SensorRecord> recs = batch.getRecords();
                            String intervalo = recs.isEmpty() ? "-" : (recs.get(0).getSeq() + ".." + recs.get(recs.size() - 1).getSeq());
                            String detalhe = "inc=" + batch.getInc() + ";hb=" + batch.getHb() + ";ultimoSeq=" + batch.getLastSeq() + ";n=" + recs.size() + ";seqs=" + intervalo;
                            String linhaRecebido = System.currentTimeMillis() + ";RECEBIDO;p" + k + ";" + detalhe;
                            System.out.println(linhaRecebido);
                            logMonitor(id, linhaRecebido);

                            for (SensorRecord r : recs) {
                                long seq = r.getSeq();

                                if (sDescartar != -1 && seq == sDescartar && !state.descartadosSet.contains(seq)) {
                                    state.descartadosSet.add(seq);
                                    System.out.println("DESCARTADO p" + k + ": " + seq);
                                    continue;
                                }

                                if (pPerda > 0.0 && rand.nextDouble() < pPerda) {
                                    System.out.println("DESCARTADO p" + k + ": " + seq);
                                    continue;
                                }

                                try {
                                    processaRegisto(state, r);
                                } catch (IOException e) {
                                    System.err.println("Erro ao processar registo: " + e.getMessage());
                                }

                                if (state.retidos.size() > Wire.LIMITE_RETIDOS && state.lastHost != null && state.lastPort > 0) {
                                    long de = state.ultimoSeq + 1;
                                    long ate = state.retidos.firstKey() - 1;
                                    executaRecuperacao(state, de, ate, N, id);
                                }
                            }

                            if (state.ultimoSeq < batch.getLastSeq() && state.lastHost != null && state.lastPort > 0) {
                                long de = state.ultimoSeq + 1;
                                long ate = state.retidos.isEmpty() ? batch.getLastSeq() : (state.retidos.firstKey() - 1);
                                if (de <= ate) {
                                    executaRecuperacao(state, de, ate, N, id);
                                }
                            }
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Erro no subscritor da p" + k + ": " + e.getMessage());
                }
            }).start();
        }

        ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor();
        checker.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private static void processaRegisto(PartitionState state, SensorRecord r) throws IOException {
        long seq = r.getSeq();

        if (seq <= state.ultimoSeq || state.retidos.containsKey(seq)) {
            System.out.println("DUPLICADO p" + state.k + ": " + seq);
        } else if (seq == state.ultimoSeq + 1) {
            state.file.append(r);
            state.ultimoSeq = seq;
            System.out.println("Aplicado p" + state.k + ": " + r.toLine());
            desentupaCascata(state);
        } else {
            state.retidos.put(seq, r);
            System.out.println("EM ESPERA p" + state.k + ": " + seq);
        }
    }

    private static void desentupaCascata(PartitionState state) throws IOException {
        while (state.retidos.containsKey(state.ultimoSeq + 1)) {
            SensorRecord prox = state.retidos.remove(state.ultimoSeq + 1);
            state.file.append(prox);
            state.ultimoSeq = prox.getSeq();
            System.out.println("Cascata Aplicada p" + state.k + ": " + prox.toLine());
        }
    }

    private static void executaRecuperacao(PartitionState state, long de, long ate, int n, String replicaId) {
        String linhaRec = System.currentTimeMillis() + ";RECUPERACAO;p" + state.k + ";de=" + de + ";ate=" + ate;
        System.out.println(linhaRec);
        logMonitor(replicaId, linhaRec);

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(state.lastHost, state.lastPort), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);

            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);

            out.println("PEDIDO " + de + " " + ate);

            String line;
            boolean recebeuFim = false;
            while ((line = in.readLine()) != null) {
                if ("FIM".equals(line)) {
                    recebeuFim = true;
                    break;
                }
                if (line.startsWith("ERRO")) return;

                SensorRecord r = SensorRecord.fromLine(line, n);
                if (r.getSeq() == state.ultimoSeq + 1) {
                    state.file.append(r);
                    state.ultimoSeq = r.getSeq();
                } else if (r.getSeq() > state.ultimoSeq + 1) {
                    state.retidos.put(r.getSeq(), r);
                }
            }

            if (recebeuFim) {
                desentupaCascata(state);
            }
        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU p" + state.k + ": " + e.getMessage());
        }
    }
}
