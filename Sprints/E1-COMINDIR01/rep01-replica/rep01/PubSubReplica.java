package rep01;

import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";
        String apiUrl = "http://127.0.0.1:" + port + "/api/v0";

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        IpfsPubSub ipfs = new IpfsPubSub(apiUrl);
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        Object stateLock = new Object();
        ScheduledFuture<?>[] timeoutTask = new ScheduledFuture<?>[1];

        try {
            long[] ultimoSeq = new long[]{file.lastSeq()};
            System.out.println("Réplica PubSub " + id + " ligada à API " + apiUrl + " | seq inicial: " + ultimoSeq[0]);

            long[] tPrimeiro = {0};
            long[] tUltimo = {0};
            int[] totalRecebidos = {0};
            int[] totalEscritos = {0};
            boolean[] emRajada = {false};

            ipfs.subscribe(TOPIC, msg -> {
                synchronized (stateLock) {
                    long tAgora = System.currentTimeMillis();
                    if (!emRajada[0]) {
                        emRajada[0] = true;
                        tPrimeiro[0] = tAgora;
                        totalRecebidos[0] = 0;
                        totalEscritos[0] = 0;
                    }
                    tUltimo[0] = tAgora;
                    totalRecebidos[0]++;

                    if (timeoutTask[0] != null) {
                        timeoutTask[0].cancel(false);
                    }
                    timeoutTask[0] = timer.schedule(() -> {
                        synchronized (stateLock) {
                            if (emRajada[0]) {
                                System.out.println("\n=== RESUMO DA RAJADA PUBSUB ===");
                                System.out.println("T_primeiro: " + tPrimeiro[0] + " ms");
                                System.out.println("T_ultimo: " + tUltimo[0] + " ms");
                                System.out.println("Duracao: " + (tUltimo[0] - tPrimeiro[0]) + " ms");
                                System.out.println("Recebidos: " + totalRecebidos[0]);
                                System.out.println("Escritos em ficheiro: " + totalEscritos[0]);
                                System.out.println("Retidos em memoria: " + retidos.size());
                                System.out.println("=================================\n");
                                emRajada[0] = false;
                                timeoutTask[0] = null;
                            }
                        }
                    }, 3, TimeUnit.SECONDS);

                    SensorRecord r;
                    try {
                        r = SensorRecord.fromLine(msg);
                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg);
                        return;
                    }

                    try {
                        long seq = r.getSeq();
                        if (seq <= ultimoSeq[0] || retidos.containsKey(seq)) {
                            System.out.println("DUPLICADO: " + seq);
                        } else if (seq == ultimoSeq[0] + 1) {
                            file.append(r);
                            ultimoSeq[0] = seq;
                            totalEscritos[0]++;
                            System.out.println("Aplicado: " + r.toLine());

                            while (retidos.containsKey(ultimoSeq[0] + 1)) {
                                SensorRecord prox = retidos.remove(ultimoSeq[0] + 1);
                                file.append(prox);
                                ultimoSeq[0] = prox.getSeq();
                                totalEscritos[0]++;
                                System.out.println("Cascata Aplicada: " + prox.toLine());
                            }
                        } else {
                            retidos.put(seq, r);
                            System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimoSeq[0] + 1) + ")");
                        }
                    } catch (Exception e) {
                        System.out.println("Erro ao processar ficheiro: " + e.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            System.out.println("Erro na Réplica PubSub: " + e.getMessage());
        } finally {
            timer.shutdownNow();
        }
    }
}