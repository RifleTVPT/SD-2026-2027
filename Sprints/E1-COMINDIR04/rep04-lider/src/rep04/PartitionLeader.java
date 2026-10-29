package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class PartitionLeader {

    private static final Object stateLock = new Object();

    public static void main(String[] args) {
        int k = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        int N = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        int porto = Wire.PORTO_BASE + k;
        IpfsPubSub ps = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider-p" + k + ".txt");
        Partitioner partitioner = new Partitioner(N);

        long inc = System.currentTimeMillis();
        long[] hb = new long[]{0};

        long[] lastSeq = new long[1];
        long[] lamport = new long[1];
        VectorClock vector = new VectorClock(N);

        List<SensorRecord> pendentes = new ArrayList<>();

        try {
            SensorRecord lastRecord = file.last(N);
            if (lastRecord != null) {
                lastSeq[0] = lastRecord.getSeq();
                lamport[0] = lastRecord.getLamport();
                vector = lastRecord.getVector().copy();
            }
        } catch (IOException e) {
            System.out.println("Erro ao inicializar ficheiro: " + e.getMessage());
            return;
        }

        final VectorClock currentVector = vector;

        System.out.println("Líder p" + k + " pronto (COMINDIR04). Incarnação: " + inc + " | Porto TCP: " + porto);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                List<SensorRecord> aEnviar;
                long seqAtual;
                long hbAtual;

                synchronized (stateLock) {
                    aEnviar = new ArrayList<>(pendentes);
                    pendentes.clear();
                    seqAtual = lastSeq[0];
                    hb[0]++;
                    hbAtual = hb[0];
                }

                Batch batch = new Batch(host, porto, inc, hbAtual, seqAtual, aEnviar);
                String msgLote = batch.encode();
                byte[] bytesLote = msgLote.getBytes(StandardCharsets.UTF_8);

                ps.publish(Wire.topicData(k), msgLote);

                String linhaHb = inc + ";" + hbAtual + ";" + System.currentTimeMillis() + ";" +
                                 seqAtual + ";" + aEnviar.size() + ";" + bytesLote.length + System.lineSeparator();
                Files.writeString(Paths.get("hb-p" + k + ".txt"), linhaHb, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);

                System.out.println("HEARTBEAT p" + k + ": hb=" + hbAtual + " n=" + aEnviar.size() + " ultimoSeq=" + seqAtual);

            } catch (Exception e) {
                System.out.println("Erro ao enviar lote em p" + k + ": " + e.getMessage());
            }
        }, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k), msg -> {
                    try {
                        if (!"IN".equals(Wire.type(msg))) return;

                        String[] f = Wire.body(msg).split(";");
                        if (f.length != 4) return;

                        String sensor = f[0];
                        double temp = Double.parseDouble(f[1]);
                        long lMsg = Long.parseLong(f[2]);
                        VectorClock vMsg = VectorClock.parse(f[3], N);

                        if (partitioner.of(sensor) != k) return;

                        synchronized (stateLock) {
                            lamport[0] = Math.max(lamport[0], lMsg) + 1;
                            currentVector.merge(vMsg);
                            currentVector.tick(k);
                            lastSeq[0]++;

                            if (currentVector.get(k) != lastSeq[0]) {
                                throw new IllegalStateException("Violação V[k] == seq: " + currentVector.get(k) + " != " + lastSeq[0]);
                            }

                            SensorRecord r = SensorRecord.now(lastSeq[0], sensor, temp, lamport[0], currentVector.copy());
                            file.append(r);
                            pendentes.add(r);
                        }
                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
                    }
                });
            } catch (Exception e) {
                System.out.println("Erro na thread de entrada da p" + k + ": " + e.getMessage());
            }
        }).start();

        try (ServerSocket server = new ServerSocket(porto)) {
            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();

                    if (pedido == null || !pedido.startsWith("PEDIDO ")) {
                        out.println("ERRO Pedido inválido");
                        continue;
                    }

                    String[] parts = pedido.split("\\s+");
                    if (parts.length != 3) {
                        out.println("ERRO Formato inválido");
                        continue;
                    }

                    long de = Long.parseLong(parts[1]);
                    long ate = Long.parseLong(parts[2]);

                    if (de > ate) {
                        out.println("ERRO Intervalo inválido (de > até)");
                        continue;
                    }

                    long maxSeq;
                    synchronized (stateLock) {
                        maxSeq = lastSeq[0];
                    }

                    if (ate > maxSeq) {
                        ate = maxSeq;
                    }

                    List<SensorRecord> registos = file.range(de, ate, N);
                    for (SensorRecord r : registos) {
                        out.println(r.toLine());
                    }
                    out.println("FIM");

                    System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + s.getRemoteSocketAddress() + " (" + registos.size() + " registos)");

                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Erro no ServerSocket da p" + k + ": " + e.getMessage());
        }
    }
}
