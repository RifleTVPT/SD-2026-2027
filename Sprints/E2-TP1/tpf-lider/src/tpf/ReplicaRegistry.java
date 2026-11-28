package tpf;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Registo central e sincronizado das réplicas conhecidas pelo Líder.
 * Controla estados (VIVA, SUSPEITA, MORTA), quotas, bytes usados, reservas em curso e exclusões.
 */
public final class ReplicaRegistry {

    public static final class InfoReplica {
        public final String id;
        public final HeartbeatMonitor monitor;
        public long quota;
        public long usados;
        public long reservados;
        public boolean removida;

        public InfoReplica(String id, long agoraMs) {
            this.id = id;
            this.monitor = new HeartbeatMonitor(agoraMs);
        }

        public long livreEfetivo() {
            return Math.max(0L, quota - usados - reservados);
        }
    }

    private final Map<String, InfoReplica> replicas = new LinkedHashMap<>();
    private final Placement placement = new Placement(Wire.ALFA, Wire.MARGEM_BYTES);
    private final BiConsumer<String, String> eventLogger;

    public ReplicaRegistry(BiConsumer<String, String> eventLogger) {
        this.eventLogger = eventLogger;
    }

    public synchronized void registarOuAtualizar(String id) {
        replicas.computeIfAbsent(id, k -> new InfoReplica(k, System.currentTimeMillis())).removida = false;
    }

    public synchronized void removerReplica(String id) {
        InfoReplica info = replicas.get(id);
        if (info != null) {
            info.removida = true;
            info.reservados = 0;
        }
    }

    public synchronized void onHeartbeat(String id, long usados, long quota) {
        long agora = System.currentTimeMillis();
        InfoReplica info = replicas.computeIfAbsent(id, k -> new InfoReplica(k, agora));
        if (info.removida) return;

        HeartbeatMonitor.Estado antigo = info.monitor.getEstado();
        info.monitor.registarHeartbeat(agora);
        info.usados = usados;
        info.quota = quota;

        if (antigo != HeartbeatMonitor.Estado.VIVA) {
            eventLogger.accept("ESTADO", id + " " + antigo + " -> VIVA");
        }
    }

    public synchronized void atualizarEstados(long agora) {
        for (InfoReplica info : replicas.values()) {
            if (info.removida) continue;
            HeartbeatMonitor.Estado antigo = info.monitor.getEstado();
            HeartbeatMonitor.Estado novo = info.monitor.avaliar(agora);
            if (antigo != novo) {
                eventLogger.accept("ESTADO", info.id + " " + antigo + " -> " + novo);
            }
        }
    }

    /**
     * Escolhe até 'count' réplicas e soma imediatamente 'tamanho' aos reservados de cada escolhida,
     * tudo dentro da mesma secção sincronizada para impedir ultrapassagem de quota concorrente.
     */
    public synchronized List<String> reserveAndChoose(String chunkId, long tamanho, int count, Set<String> exclude) {
        List<Placement.Candidate> candidatos = new ArrayList<>();
        for (InfoReplica info : replicas.values()) {
            if (info.removida) continue;
            if (info.monitor.getEstado() != HeartbeatMonitor.Estado.MORTA) {
                candidatos.add(new Placement.Candidate(info.id, info.livreEfetivo()));
            }
        }

        List<String> escolhidas = placement.choose(chunkId, tamanho, count, candidatos, exclude);
        for (String id : escolhidas) {
            InfoReplica info = replicas.get(id);
            if (info != null) {
                info.reservados += tamanho;
            }
        }
        return escolhidas;
    }

    /**
     * Chamado quando chega um ACK válido: converte a reserva em espaço usado.
     */
    public synchronized void confirmarReserva(String replicaId, long tamanho) {
        InfoReplica info = replicas.get(replicaId);
        if (info != null) {
            info.reservados = Math.max(0L, info.reservados - tamanho);
            info.usados += tamanho;
        }
    }

    /**
     * Chamado quando expira o prazo ACK_TIMEOUT_MS sem confirmação.
     */
    public synchronized void libertar(String replicaId, long tamanho) {
        InfoReplica info = replicas.get(replicaId);
        if (info != null) {
            info.reservados = Math.max(0L, info.reservados - tamanho);
        }
    }

    public synchronized List<InfoReplica> snapshot() {
        List<InfoReplica> copy = new ArrayList<>();
        for (InfoReplica r : replicas.values()) {
            if (!r.removida) copy.add(r);
        }
        return copy;
    }
}