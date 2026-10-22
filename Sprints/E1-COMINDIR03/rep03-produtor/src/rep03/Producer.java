package rep03;

import java.util.Random;
import java.util.Scanner;

public class Producer {
    public static void main(String[] args) {
        int nParticoes = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(nParticoes);
        IpfsPubSub ipfs = new IpfsPubSub(api);

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

                    if (parts.length != 3) {
                        System.out.println("Formato inválido. Use: rajada <n> <s> ou rajada-quente <n> <s>");
                        continue;
                    }

                    int n = Integer.parseInt(parts[1]);
                    int s = Integer.parseInt(parts[2]);

                    if (quente && s < 2) {
                        System.out.println("Para rajada-quente, s deve ser pelo menos 2.");
                        continue;
                    }

                    Random rand = new Random();
                    int[] contagemParticoes = new int[nParticoes];

                    long tInicio = System.currentTimeMillis();
                    long tFim = tInicio;

                    for (int i = 0; i < n; i++) {
                        String sensor;
                        if (quente && rand.nextDouble() < 0.8) {
                            sensor = "S01";
                        } else {
                            int minSensor = quente ? 2 : 1;
                            int numSensor = minSensor + rand.nextInt(s - minSensor + 1);
                            sensor = String.format("S%02d", numSensor);
                        }

                        double temp = 15.0 + (15.0 * rand.nextDouble());
                        temp = Math.round(temp * 10.0) / 10.0;

                        int particao = partitioner.of(sensor);
                        contagemParticoes[particao]++;

                        // Produtor envia sempre L = 0 e V = (0, ..., 0)
                        String msg = Wire.in(sensor, temp, 0, new VectorClock(nParticoes));
                        ipfs.publish(Wire.topicIn(particao), msg);
                        tFim = System.currentTimeMillis();
                    }

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
                String msg = Wire.in(sensor, temp, 0, new VectorClock(nParticoes));
                ipfs.publish(Wire.topicIn(particao), msg);

                System.out.println("Enviado: " + msg + " -> p" + particao);
            }
        } catch (Exception e) {
            System.out.println("Erro no Produtor: " + e.getMessage());
        }
    }
}