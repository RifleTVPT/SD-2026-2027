package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            System.out.println("Líder Multicast pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");

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
                            byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                            if (i == 0) tInicio = System.currentTimeMillis();
                            // BLOQUEIA POTENCIALMENTE: envia sincronicamente para o buffer local do SO.
                            socket.send(new DatagramPacket(m, m.length, group, PORT));
                            tFim = System.currentTimeMillis();
                        }
                        System.out.println("Rajada enviada! T_inicio: " + tInicio + " ms | T_fim: " + tFim + " ms | Duracao: " + (tFim - tInicio) + " ms");
                    } catch (NumberFormatException e) {
                        System.out.println("Número de rajada inválido.");
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
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                // BLOQUEIA POTENCIALMENTE: envia sincronicamente para o buffer local do SO.
                socket.send(new DatagramPacket(m, m.length, group, PORT));
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}