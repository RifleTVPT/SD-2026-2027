package tpf;

/**
 * Monitor Individual do Estado de Vitalidade de uma Réplica Voluntária.
 * 
 * PAPEL NO SISTEMA:
 * Mantém e transita o estado de saúde de uma réplica individual com base no timestamp
 * do último Heartbeat (HB) recebido pelo Líder:
 *   - VIVA: Réplica ativa a responder dentro do intervalo normal (< SUSPEITA_MS, ou seja, < 6 s).
 *   - SUSPEITA: Falhou 3 heartbeats consecutivos (>= SUSPEITA_MS e < MORTA_MS, entre 6 s e 10 s).
 *               Ainda pode ser selecionada se não houver outras, mas é candidata a falha.
 *   - MORTA: Falhou 5 heartbeats consecutivos (>= MORTA_MS, >= 10 s).
 *            Totalmente excluída de novas atribuições de chunks em Placement.choose.
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - Acedido concorrentemente por duas fontes de threads:
 *   1. Thread do WebSocket (PubSubHub): Chama registarHeartbeat() quando chega a mensagem 'PUB;tpf-lider;HB;...'.
 *   2. Thread do Agendador (LeaderBot scheduler): Chama avaliar() a cada 1 000 ms para atualizar transições.
 * - Sincronização via 'synchronized(this)' em todos os métodos, garantindo consistência atómica
 *   entre o timestamp registado e a transição do enum Estado.
 * 
 * ONDE É UTILIZADA:
 * - ReplicaRegistry.InfoReplica: cada réplica registada tem a sua instância própria de HeartbeatMonitor.
 */
public final class HeartbeatMonitor {

    /**
     * Estados possíveis do ciclo de vida de uma réplica.
     */
    public enum Estado {
        VIVA, SUSPEITA, MORTA
    }

    private Estado estado = Estado.VIVA;
    private long ultimoHbMs;

    /**
     * Cria o monitor inicializando o estado a VIVA com o timestamp atual.
     *
     * @param agoraMs Timestamp do momento da ligação ou registo
     */
    public HeartbeatMonitor(long agoraMs) {
        this.ultimoHbMs = agoraMs;
    }

    /**
     * Atualiza o timestamp do último heartbeat e repõe imediatamente o estado a VIVA.
     * Chamado quando o líder recebe com sucesso um 'HB' da réplica.
     * 
     * Onde é utilizado: ReplicaRegistry.onHeartbeat().
     *
     * @param agoraMs Timestamp atual do sistema
     */
    public synchronized void registarHeartbeat(long agoraMs) {
        this.ultimoHbMs = agoraMs;
        this.estado = Estado.VIVA;
    }

    /**
     * Avalia o tempo decorrido desde o último heartbeat e transita o estado:
     * - Se decorridos >= Wire.MORTA_MS (10 s) -> MORTA
     * - Se decorridos >= Wire.SUSPEITA_MS (6 s) -> SUSPEITA
     * - Caso contrário -> VIVA
     * 
     * Onde é utilizado: ReplicaRegistry.atualizarEstados() executado periodicamente.
     *
     * @param agoraMs Timestamp do instante de avaliação
     * @return O novo estado calculado
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

    /**
     * Devolve o estado de saúde atual da réplica.
     * 
     * Onde é utilizado: LeaderBot (/estado) e ReplicaRegistry (reserveAndChoose).
     */
    public synchronized Estado getEstado() {
        return estado;
    }

    /**
     * Devolve o instante em milissegundos do último heartbeat recebido.
     */
    public synchronized long getUltimoHbMs() {
        return ultimoHbMs;
    }
}
