package rep03;

import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {

    private static final Object stateLock = new Object();

    public static void main(String[] args) {
        int N = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ipfs = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(N);

        long[] lp = new long[1];
        VectorClock vp = new VectorClock(N);
        Set<String> processados = new HashSet<>();

        System.out.println("Processador de Alarmes iniciado com N=" + N + " na API " + api);

        for (int k = 0; k < N; k++) {
            final int particaoOrigem = k;
            new Thread(() -> {
                try {
                    ipfs.subscribe(Wire.topicData(particaoOrigem), msg -> {
                        try {
                            String type = Wire.type(msg);
                            if (!"REG".equals(type)) return;

                            String[] parts = Wire.regParts(msg);
                            SensorRecord r = SensorRecord.fromLine(parts[2], N);

                            // Ignorar alarmes sobre alarmes
                            if (r.getSensorId().startsWith("ALARM-")) return;

                            String recordKey = particaoOrigem + "#" + r.getSeq();

                            long lToPublish;
                            VectorClock vToPublish;
                            boolean eAlarme = false;
                            int particaoDestino = -1;
                            String alarmSensor = "ALARM-" + r.getSensorId();

                            synchronized (stateLock) {
                                // Ignorar duplicados
                                if (processados.contains(recordKey)) return;
                                processados.add(recordKey);

                                // Atualização do conhecimento causal (acumula o máximo)
                                lp[0] = Math.max(lp[0], r.getLamport());
                                vp.merge(r.getVector());

                                if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                                    eAlarme = true;
                                    lToPublish = lp[0];
                                    vToPublish = vp.copy(); // copy() essencial para isolamento de thread
                                    particaoDestino = partitioner.of(alarmSensor);
                                } else {
                                    return;
                                }
                            }

                            if (eAlarme) {
                                String inMsg = Wire.in(alarmSensor, r.getTemperature(), lToPublish, vToPublish);
                                ipfs.publish(Wire.topicIn(particaoDestino), inMsg);
                                System.out.println("ALARME: " + r.getSensorId() + " p" + particaoOrigem + "#" + r.getSeq() + " -> p" + particaoDestino);
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