package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Scanner;
import java.util.concurrent.*;

public class Producer {

    private static final Object logLock = new Object();

    private static void logProdutor(String linha) {
        synchronized (logLock) {
            try {
                Files.writeString(Paths.get("produtor.txt"),
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

        HeartbeatMonitor[] monitors = new HeartbeatMonitor[N];
        for (int k = 0; k < N; k++) {
            final int part = k;
            monitors[k] = new HeartbeatMonitor(part, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, evt -> {
                String linha = System.currentTimeMillis() + ";" + evt;
                try {
                    Files.writeString(Paths.get("monitor-produtor.txt"), linha + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException ignored) {}
            });
        }

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitors) m.check();
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        for (int k = 0; k < N; k++) {
            final int part = k;
            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(part), msg -> {
                        if ("LOTE".equals(Wire.type(msg))) {
                            Batch b = Batch.decode(msg, N);
                            monitors[part].onHeartbeat(b.getTerm(), b.getLeader(), b.getHb());
                        }
                    });
                } catch (Exception ignored) {}
            }).start();
        }

        Scanner sc = new Scanner(System.in);
        System.out.println("Produtor Raft pronto. Comandos:");
        System.out.println("  <sensor> <temperatura>");
        System.out.println("  rajada <n> <intervaloMs> [sensor]");
        System.out.println("  status");

        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.isBlank()) continue;
            String[] f = line.split("\\s+");

            if ("status".equalsIgnoreCase(f[0])) {
                System.out.println("=== STATUS MONITORES PRODUTOR ===");
                for (int k = 0; k < N; k++) {
                    System.out.println("  p" + k + ": " + monitors[k].state() + " (termo=" + monitors[k].term() + ", lider=n" + monitors[k].leader() + ")");
                }
                continue;
            }

            if ("rajada".equalsIgnoreCase(f[0]) && f.length >= 3) {
                int total = Integer.parseInt(f[1]);
                long intervalo = Long.parseLong(f[2]);
                String sensorRajada = f.length > 3 ? f[3] : "S02";

                new Thread(() -> {
                    System.out.println("Iniciando rajada de " + total + " leituras para " + sensorRajada + " (intervalo=" + intervalo + "ms)...");
                    int targetK = partitioner.of(sensorRajada);
                    for (int seqNum = 1; seqNum <= total; seqNum++) {
                        double temp = 20.0 + (seqNum % 15);
                        enviarLeitura(ipfs, monitors, targetK, sensorRajada, temp, N);
                        if (intervalo > 0) {
                            try { Thread.sleep(intervalo); } catch (InterruptedException ignored) {}
                        }
                    }
                    System.out.println("Rajada concluída!");
                }).start();
                continue;
            }

            if (f.length == 2) {
                String sensor = f[0];
                double temp = Double.parseDouble(f[1]);
                int targetK = partitioner.of(sensor);
                enviarLeitura(ipfs, monitors, targetK, sensor, temp, N);
            }
        }
    }

    private static void enviarLeitura(IpfsPubSub ipfs, HeartbeatMonitor[] monitors, int targetK, String sensor, double temp, int N) {
        if (monitors[targetK].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: partição p" + targetK + " sem líder conhecido; leitura " + sensor + " pode perder-se");
        }

        VectorClock emptyV = new VectorClock(N);
        String inMsg = Wire.in(sensor, temp, 0, emptyV);
        try {
            ipfs.publish(Wire.topicIn(targetK), inMsg);
            logProdutor(System.currentTimeMillis() + ";p" + targetK + ";" + sensor + ";" + temp);
        } catch (Exception ignored) {}
    }
}
