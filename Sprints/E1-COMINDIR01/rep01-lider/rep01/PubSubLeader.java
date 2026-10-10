package rep01;

import java.util.Random;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String apiUrl = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ipfs = new IpfsPubSub(apiUrl);
        RecordFile file = new RecordFile("lider.txt");

        try (Scanner sc = new Scanner(System.in)) {
            long seq = file.lastSeq();
            System.out.println("Líder PubSub pronto na API " + apiUrl + ". Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");

            while (true) {
                System.out.print("> ");
                // BLOQUEIA: espera uma nova linha introduzida pelo utilizador na CLI.
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                if (line.toLowerCase().startsWith("rajada ")) {
                    try {
                        int n = Integer.parseInt(line.substring(7).trim());
                        Random rand = new Random();
                        long tInicio = 0;
                        long tFim = 0;
                        for (int i = 0; i < n; i++) {
                            double temp = 15.0 + (15.0 * rand.nextDouble());
                            SensorRecord r = SensorRecord.now(++seq, "SIM", Math.round(temp * 10.0) / 10.0);
                            file.append(r);
                            if (i == 0) tInicio = System.currentTimeMillis();
                            ipfs.publish(TOPIC, r.toLine());
                            tFim = System.currentTimeMillis();
                        }
                        System.out.println("Rajada enviada! T_inicio: " + tInicio + " ms | T_fim: " + tFim + " ms | Duracao: " + (tFim - tInicio) + " ms");
                    } catch (Exception e) {
                        System.out.println("Erro na rajada: " + e.getMessage());
                    }
                    continue;
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida.");
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);
                ipfs.publish(TOPIC, r.toLine());
                System.out.println("Registado e publicado: " + r.toLine());
            }
        } catch (Exception e) {
            System.out.println("Erro no Líder PubSub: " + e.getMessage());
        }
    }
}