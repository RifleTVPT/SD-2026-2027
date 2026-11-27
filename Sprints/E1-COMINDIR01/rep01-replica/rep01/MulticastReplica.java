package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> retidos = new TreeMap<>();

        try (MulticastSocket socket = new MulticastSocket(PORT)) 
        {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            socket.setSoTimeout(3000);
            long ultimoSeq = file.lastSeq();
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT + " | seq inicial: " + ultimoSeq);

            byte[] buffer = new byte[1000];
            boolean emRajada = false;
            long tPrimeiro = 0;
            long tUltimo = 0;
            int totalRecebidos = 0;
            int totalEscritos = 0;

            while (true) {
                try {
                    DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                    // BLOQUEIA: espera um datagrama UDP enviado para o grupo multicast.
                    socket.receive(p);
                    long tAgora = System.currentTimeMillis();
                    if (!emRajada) {
                        emRajada = true;
                        tPrimeiro = tAgora;
                    }
                    tUltimo = tAgora;
                    totalRecebidos++;

                    String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                    // Valida o conteúdo recebido como um registo textual.
                    SensorRecord r;
                    try {
                        r = SensorRecord.fromLine(line);
                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + line);
                        continue;
                    }

                    long seq = r.getSeq();
                    if (seq <= ultimoSeq || retidos.containsKey(seq)) {
                        System.out.println("DUPLICADO: " + seq);
                    } else if (seq == ultimoSeq + 1) {
                        file.append(r);
                        ultimoSeq = seq;
                        totalEscritos++;
                        System.out.println("Aplicado: " + r.toLine());

                        while (retidos.containsKey(ultimoSeq + 1)) {
                            SensorRecord prox = retidos.remove(ultimoSeq + 1);
                            file.append(prox);
                            ultimoSeq = prox.getSeq();
                            totalEscritos++;
                            System.out.println("Cascata Aplicada: " + prox.toLine());
                        }
                    } else {
                        retidos.put(seq, r);
                        System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimoSeq + 1) + ")");
                    }

                    if (totalRecebidos % 100 == 0 || totalRecebidos == 1) {
                        System.out.println("[METRICAS] Recebidos: " + totalRecebidos + " | Escritos: " + totalEscritos + " | Retidos: " + retidos.size() + " | T_delta: " + (tUltimo - tPrimeiro) + " ms");
                    }
                } catch (SocketTimeoutException e) {
                    if (emRajada) {
                        System.out.println("\n=== RESUMO DA RAJADA ===");
                        System.out.println("T_primeiro: " + tPrimeiro + " ms");
                        System.out.println("T_ultimo: " + tUltimo + " ms");
                        System.out.println("Duracao: " + (tUltimo - tPrimeiro) + " ms");
                        System.out.println("Recebidos: " + totalRecebidos);
                        System.out.println("Escritos em ficheiro: " + totalEscritos);
                        System.out.println("Retidos em memoria: " + retidos.size());
                        System.out.println("========================\n");

                        emRajada = false;
                        totalRecebidos = 0;
                        totalEscritos = 0;
                        tPrimeiro = 0;
                        tUltimo = 0;
                    }
                } catch (IOException e) {
                    System.out.println("Erro de E/S na réplica: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}