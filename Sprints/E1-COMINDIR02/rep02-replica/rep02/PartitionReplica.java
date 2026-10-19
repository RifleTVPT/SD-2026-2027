package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PartitionReplica {

    private static class PartitionState {
        final int k;
        final RecordFile file;
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        final Set<Long> descartadosSet = new HashSet<>();
        long ultimoSeq;
        String lastHost;
        int lastPort = -1;

        PartitionState(int k, String replicaId) throws IOException {
            this.k = k;
            this.file = new RecordFile("replica-" + replicaId + "-p" + k + ".txt");
            this.ultimoSeq = file.lastSeq();
        }
    }

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String[] particoesStr = args.length > 1 ? args[1].split(",") : new String[]{"0"};
        String apiPort = args.length > 2 ? args[2] : "5001";
        String apiUrl = "http://127.0.0.1:" + apiPort + "/api/v0";

        double probPerda = 0.0;
        String probPerdaProp = System.getProperty("perda");
        if (probPerdaProp != null) {
            probPerda = Double.parseDouble(probPerdaProp);
        }

        long seqDescartar = -1;
        String descartarProp = System.getProperty("descartar");
        if (descartarProp != null) {
            seqDescartar = Long.parseLong(descartarProp);
        }

        final double pPerda = probPerda;
        final long sDescartar = seqDescartar;

        IpfsPubSub ipfs = new IpfsPubSub(apiUrl);
        Random rand = new Random();

        for (String pStr : particoesStr) {
            int k = Integer.parseInt(pStr.trim());

            new Thread(() -> {
                PartitionState state;
                try {
                    state = new PartitionState(k, id);
                } catch (IOException e) {
                    System.out.println("Erro ao criar estado da p" + k + ": " + e.getMessage());
                    return;
                }

                System.out.println("Réplica " + id + " a ouvir p" + k + " | seq inicial: " + state.ultimoSeq);

                try {
                    ipfs.subscribe(Wire.topicData(k), msg -> {
                        synchronized (state) {
                            try {
                                String type = Wire.type(msg);
                                if (!"REG".equals(type)) {
                                    System.out.println("REJEITADO p" + k + ": " + msg);
                                    return;
                                }

                                String[] parts = Wire.regParts(msg);
                                state.lastHost = parts[0];
                                state.lastPort = Integer.parseInt(parts[1]);

                                SensorRecord r = SensorRecord.fromLine(parts[2]);
                                long seq = r.getSeq();

                                if (sDescartar != -1 && seq == sDescartar && !state.descartadosSet.contains(seq)) {
                                    state.descartadosSet.add(seq);
                                    System.out.println("DESCARTADO (simulação) p" + k + ": " + seq);
                                    return;
                                }

                                if (pPerda > 0.0 && rand.nextDouble() < pPerda) {
                                    System.out.println("DESCARTADO (simulação): " + seq);
                                    return;
                                }

                                processaRegisto(state, r);
                                if (state.retidos.size() > Wire.LIMITE_RETIDOS
                                        && state.lastHost != null && state.lastPort > 0) {
                                    executaRecuperacao(state);
                                }

                            } catch (Exception e) {
                                System.out.println("REJEITADO p" + k + ": " + msg);
                            }
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Erro no subscritor da p" + k + ": " + e.getMessage());
                }
            }).start();
        }
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
            System.out.println("EM ESPERA p" + state.k + ": " + seq + " (falta o registo " + (state.ultimoSeq + 1) + ")");

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

    private static void executaRecuperacao(PartitionState state) {
        long de = state.ultimoSeq + 1;
        long ate = state.retidos.firstKey() - 1;

        if (de > ate) return;

        System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + state.lastHost + ":" + state.lastPort);

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
                if (line.startsWith("ERRO")) {
                    System.out.println("RECUPERAÇÃO FALHOU p" + state.k + ": " + line);
                    return;
                }

                SensorRecord r = SensorRecord.fromLine(line);
                if (r.getSeq() == state.ultimoSeq + 1) {
                    state.file.append(r);
                    state.ultimoSeq = r.getSeq();
                    System.out.println("RECUPERADO p" + state.k + ": " + r.getSeq());
                }
            }

            if (!recebeuFim) {
                System.out.println("RECUPERAÇÃO FALHOU p" + state.k + ": Resposta incompleta (sem FIM)");
                return;
            }

            desentupaCascata(state);

        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU p" + state.k + ": " + e.getMessage());
        }
    }
}