package raft04;

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
            } catch (IOException ignored) {}
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
        long[] maxTerms = new long[N];

        HeartbeatMonitor[] monitors = new HeartbeatMonitor[N];
        for (int k = 0; k < N; k++) {
            final int part = k;
            monitors[k] = new HeartbeatMonitor(part, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, evt -> {
                String linha = System.currentTimeMillis() + ";" + evt;
                logMonitor(linha);
            });
        }

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitors) m.check();
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        for (int k = 0; k < N; k++) {
            final int partOrigem = k;
            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(partOrigem), msg -> {
                        if (!"LOTE".equals(Wire.type(msg))) return;
                        Batch b = Batch.decode(msg, N);

                        synchronized (stateLock) {
                            if (b.getTerm() < maxTerms[partOrigem]) return;
                            maxTerms[partOrigem] = b.getTerm();
                        }

                        monitors[partOrigem].onHeartbeat(b.getTerm(), b.getLeader(), b.getHb());

                        for (SensorRecord r : b.getRecords()) {
                            if (r.getSensorId().startsWith("ALARM-")) continue;
                            String pairKey = r.getSeq() + "#" + r.getTerm();

                            long lToPublish;
                            VectorClock vToPublish;
                            boolean isAlarm = false;
                            int targetK = -1;
                            String alarmSensor = "ALARM-" + r.getSensorId();

                            synchronized (stateLock) {
                                if (processados.contains(pairKey)) continue;
                                processados.add(pairKey);

                                lp[0] = Math.max(lp[0], r.getLamport());
                                vp.merge(r.getVector());

                                if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                                    isAlarm = true;
                                    lToPublish = lp[0];
                                    vToPublish = vp.copy();
                                    targetK = partitioner.of(alarmSensor);
                                } else continue;
                            }

                            if (isAlarm) {
                                if (monitors[targetK].state() == HeartbeatMonitor.State.SUSPEITO) {
                                    System.out.println("AVISO: partição p" + targetK + " sem líder conhecido; alarme " + r.getSensorId() + " pode perder-se");
                                }
                                String inMsg = Wire.in(alarmSensor, r.getTemperature(), lToPublish, vToPublish);
                                try {
                                    ipfs.publish(Wire.topicIn(targetK), inMsg);
                                } catch (Exception ignored) {}
                            }
                        }
                    });
                } catch (Exception ignored) {}
            }).start();
        }
    }
}
