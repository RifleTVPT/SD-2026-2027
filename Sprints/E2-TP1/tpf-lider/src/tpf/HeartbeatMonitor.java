package tpf;

/**
 * Monitor individual de estado de uma réplica com base nos heartbeats recebidos.
 * Estados possíveis: VIVA -> SUSPEITA (após SUSPEITA_MS) -> MORTA (após MORTA_MS).
 */
public final class HeartbeatMonitor {

    public enum Estado {
        VIVA, SUSPEITA, MORTA
    }

    private Estado estado = Estado.VIVA;
    private long ultimoHbMs;

    public HeartbeatMonitor(long agoraMs) {
        this.ultimoHbMs = agoraMs;
    }

    public synchronized void registarHeartbeat(long agoraMs) {
        this.ultimoHbMs = agoraMs;
        this.estado = Estado.VIVA;
    }

    /**
     * Avalia o tempo decorrido desde o último heartbeat e devolve o novo estado.
     */
    public synchronized Estado avaliar(long agoraMs) {
        long semHb = agoraMs - ultimoHbMs;
        if (semHb >= Wire.MORTA_MS) {
            estado = Estado.MORTA;
        } else if (semHb >= Wire.SUSPEITA_MS) {
            estado = Estado.SUSPEITA;
        } else {
            estado = Estado.VIVA;
        }
        return estado;
    }

    public synchronized Estado getEstado() {
        return estado;
    }

    public synchronized long getUltimoHbMs() {
        return ultimoHbMs;
    }
}