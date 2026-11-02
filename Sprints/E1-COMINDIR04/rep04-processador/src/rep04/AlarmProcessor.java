package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class AlarmProcessor {

    private static final Object stateLock = new Object();
    private static final Object logLock = new Object();

    private static void logMonitor(String linha) {
        synchronized (logLock) {
            try {
                Files.writeString(Paths.get("monitor-processador.txt"),
                        linha + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Erro ao escrever no monitor do processador: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ipfs = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(N);

        long[] lp = new long[1];
        VectorClock vp = new VectorClock(N);
        Set<String> processados = new HashSet<>();

        HeartbeatMonitor[] monitors = new HeartbeatMonitor[N];
        for (int k = 0; k < N; k++) {
            final int particao = k;
            monitors[k] = new HeartbeatMonitor(particao, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, evento -> {
                String linha = System.currentTimeMillis() + ";" + evento;
                System.out.println(linha);
                logMonitor(linha);
            });
        }

        ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor();
        checker.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitors) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        System.out.println("Processador de Alarmes iniciado com N=" + N + " na API " + api);

        for (int k = 0; k < N; k++) {
            final int particaoOrigem = k;

            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(particaoOrigem), msg -> {
                        try {
                            Batch batch;
                            try {
                                batch = Batch.decode(msg, N);
                            } catch (Exception e) {
                                return;
                            }

                            monitors[particaoOrigem].onHeartbeat(batch.getInc(), batch.getHb());

                            List<SensorRecord> recs = batch.getRecords();
                            String intervalo = recs.isEmpty() ? "-" : (recs.get(0).getSeq() + ".." + recs.get(recs.size() - 1).getSeq());
                            String detalhe = "inc=" + batch.getInc() + ";hb=" + batch.getHb() + ";ultimoSeq=" + batch.getLastSeq() + ";n=" + recs.size() + ";seqs=" + intervalo;
                            String linhaRecebido = System.currentTimeMillis() + ";RECEBIDO;p" + particaoOrigem + ";" + detalhe;
                            System.out.println(linhaRecebido);
                            logMonitor(linhaRecebido);

                            for (SensorRecord r : recs) {
                                if (r.getSensorId().startsWith("ALARM-")) continue;

                                String recordKey = particaoOrigem + "#" + r.getSeq();

                                long lToPublish;
                                VectorClock vToPublish;
                                boolean eAlarme = false;
                                int particaoDestino = -1;
                                String alarmSensor = "ALARM-" + r.getSensorId();

                                synchronized (stateLock) {
                                    if (processados.contains(recordKey)) continue;
                                    processados.add(recordKey);

                                    lp[0] = Math.max(lp[0], r.getLamport());
                                    vp.merge(r.getVector());

                                    if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                                        eAlarme = true;
                                        lToPublish = lp[0];
                                        vToPublish = vp.copy();
                                        particaoDestino = partitioner.of(alarmSensor);
                                    } else {
                                        continue;
                                    }
                                }

                                if (eAlarme) {
                                    if (monitors[particaoDestino].state() == HeartbeatMonitor.State.SUSPEITO) {
                                        System.out.println("AVISO: líder p" + particaoDestino + " suspeito; alarme " + r.getSensorId() + " pode perder-se");
                                    }

                                    String inMsg = Wire.in(alarmSensor, r.getTemperature(), lToPublish, vToPublish);
                                    ipfs.publish(Wire.topicIn(particaoDestino), inMsg);
                                    System.out.println("ALARME: " + r.getSensorId() + " p" + particaoOrigem + "#" + r.getSeq() + " -> p" + particaoDestino);
                                }
                            }

                        } catch (Exception e) {
                            System.out.println("Erro ao processar mensagem no AlarmProcessor: " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Erro na subscrição do AlarmProcessor na p" + particaoOrigem + ": " + e.getMessage());
                }
            }).start();
        }
    }
}
