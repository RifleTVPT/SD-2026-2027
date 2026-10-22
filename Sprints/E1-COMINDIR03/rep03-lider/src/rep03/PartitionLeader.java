package rep03;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

        long[] lastSeq = new long[1];
        long[] lamport = new long[1];
        VectorClock vector = new VectorClock(N);

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

        System.out.println("Líder p" + k + " pronto. Porto TCP: " + porto + " | seq: " + lastSeq[0] + " | L: " + lamport[0] + " | V: " + currentVector);

        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k), msg -> {
                    try {
                        String type = Wire.type(msg);
                        if (!"IN".equals(type)) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        // Formato: IN;<sensor>;<temp>;<L_msg>;<V_msg>
                        String body = Wire.body(msg);
                        String[] f = body.split(";");
                        if (f.length != 4) {
                            System.out.println("REJEITADO (Campos insuficientes): " + msg);
                            return;
                        }

                        String sensor = f[0];
                        double temp = Double.parseDouble(f[1]);
                        long lMsg = Long.parseLong(f[2]);
                        VectorClock vMsg = VectorClock.parse(f[3], N);

                        int particaoEsperada = partitioner.of(sensor);
                        if (particaoEsperada != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + particaoEsperada);
                            return;
                        }

                        SensorRecord r;
                        synchronized (stateLock) {
                            // Regra de Lamport: L = max(L, L_msg) + 1
                            lamport[0] = Math.max(lamport[0], lMsg) + 1;

                            // Regra Vetorial: max componente a componente + tick(k)
                            currentVector.merge(vMsg);
                            currentVector.tick(k);

                            lastSeq[0]++;

                            // Asserção do enunciado
                            if (currentVector.get(k) != lastSeq[0]) {
                                throw new IllegalStateException("Violação V[k] == seq: " + currentVector.get(k) + " != " + lastSeq[0]);
                            }

                            r = SensorRecord.now(lastSeq[0], sensor, temp, lamport[0], currentVector.copy());
                            file.append(r);
                        }

                        String regMsg = Wire.reg(host, porto, r);
                        ps.publish(Wire.topicData(k), regMsg);
                        System.out.println("EVENTO: seq=" + r.getSeq() + " L=" + r.getLamport() + " V=" + r.getVector());

                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
                    }
                });
            } catch (Exception e) {
                System.out.println("Erro na thread de entrada da p" + k + ": " + e.getMessage());
            }
        });
        entrada.start();

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