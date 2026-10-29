package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.*;

public class Producer {

    private static final Object logLock = new Object();

    private static void logMonitor(String linha) {
        synchronized (logLock) {
            try {
                Files.writeString(Paths.get("monitor-produtor.txt"),
                        linha + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Erro ao escrever no monitor do produtor: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        int nParticoes = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(nParticoes);
        IpfsPubSub ipfs = new IpfsPubSub(api);

        HeartbeatMonitor[] monitors = new HeartbeatMonitor[nParticoes];
        for (int k = 0; k < nParticoes; k++) {
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

        for (int k = 0; k < nParticoes; k++) {
            final int particao = k;
            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(particao), msg -> {
                        try {
                            int nl = msg.indexOf('\n');
                            String headerLine = nl >= 0 ? msg.substring(0, nl).trim() : msg.trim();
                            String[] parts = headerLine.split(";");
                            if (parts.length == 7 && "LOTE".equals(parts[0])) {
                                long inc = Long.parseLong(parts[3]);
                                long hb = Long.parseLong(parts[4]);
                                long lastSeq = Long.parseLong(parts[5]);
                                int count = Integer.parseInt(parts[6]);

                                monitors[particao].onHeartbeat(inc, hb);

                                String detalhe = "inc=" + inc + ";hb=" + hb + ";ultimoSeq=" + lastSeq + ";n=" + count + ";seqs=-";
                                String linhaRecebido = System.currentTimeMillis() + ";RECEBIDO;p" + particao + ";" + detalhe;
                                System.out.println(linhaRecebido);
                                logMonitor(linhaRecebido);
                            }
                        } catch (Exception e) {

                        }
                    });
                } catch (Exception e) {
                    System.out.println("Erro na subscrição do Produtor na p" + particao + ": " + e.getMessage());
                }
            }).start();
        }

        System.out.println("Produtor iniciado com N=" + nParticoes + " partições na API " + api);

        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                if (line.toLowerCase().startsWith("rajada ") || line.toLowerCase().startsWith("rajada-quente ")) {
                    boolean quente = line.toLowerCase().startsWith("rajada-quente ");
                    String[] parts = line.split("\\s+");

                    if (parts.length < 3) {
                        System.out.println("Formato inválido. Use: rajada <n> <intervaloMs> [sensores]");
                        continue;
                    }

                    int n = Integer.parseInt(parts[1]);
                    int intervaloMs = Integer.parseInt(parts[2]);
                    int s = parts.length > 3 ? Integer.parseInt(parts[3]) : 10;

                    Random rand = new Random();
                    int[] contagemParticoes = new int[nParticoes];

                    long tInicio = System.currentTimeMillis();

                    for (int i = 0; i < n; i++) {
                        String sensor;
                        if (quente && rand.nextDouble() < 0.8) {
                            sensor = "S01";
                        } else {
                            int numSensor = 1 + rand.nextInt(Math.max(1, s));
                            sensor = String.format("S%02d", numSensor);
                        }

                        double temp = 15.0 + (15.0 * rand.nextDouble());
                        temp = Math.round(temp * 10.0) / 10.0;

                        int particao = partitioner.of(sensor);
                        contagemParticoes[particao]++;

                        if (monitors[particao].state() == HeartbeatMonitor.State.SUSPEITO) {
                            System.out.println("AVISO: líder p" + particao + " suspeito; leitura " + sensor + " pode perder-se");
                        }

                        String msg = Wire.in(sensor, temp, 0, new VectorClock(nParticoes));
                        ipfs.publish(Wire.topicIn(particao), msg);

                        if (intervaloMs > 0) {
                            try {
                                Thread.sleep(intervaloMs);
                            } catch (InterruptedException ignored) {}
                        }
                    }

                    long tFim = System.currentTimeMillis();
                    System.out.println("Rajada concluída em " + (tFim - tInicio) + " ms");
                    for (int k = 0; k < nParticoes; k++) {
                        System.out.println("  Partição p" + k + ": " + contagemParticoes[k] + " mensagens");
                    }
                    continue;
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido. Use: <sensor> <temperatura>");
                    continue;
                }

                String sensor = p[0];
                double temp = Double.parseDouble(p[1]);
                int particao = partitioner.of(sensor);

                if (monitors[particao].state() == HeartbeatMonitor.State.SUSPEITO) {
                    System.out.println("AVISO: líder p" + particao + " suspeito; leitura " + sensor + " pode perder-se");
                }

                String msg = Wire.in(sensor, temp, 0, new VectorClock(nParticoes));
                ipfs.publish(Wire.topicIn(particao), msg);
                System.out.println("Enviado: " + msg + " -> p" + particao);
            }
        } catch (Exception e) {
            System.out.println("Erro no Produtor: " + e.getMessage());
        }
    }
}
