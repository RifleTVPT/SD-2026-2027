package tpf;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Registo Central e Sincronizado das Réplicas Conhecidas pelo Líder.
 * 
 * PAPEL NO SISTEMA:
 * Mantém a vista operacional em memória de todo o cluster de réplicas voluntárias:
 * - Acompanha quotas físicas declaradas, bytes usados reportados nos heartbeats e espaço reservado em curso;
 * - Calcula o espaço livre efetivo: (quota - usados - reservados);
 * - Orquestra a transição de estados de saúde através do HeartbeatMonitor de cada réplica;
 * - Fornece a operação atómica 'reserveAndChoose' que combina a seleção de colocação (Placement)
 *   com o bloqueio prévio de espaço em disco, evitando que dois chunks selados concorrentemente
 *   ultrapassem a quota máxima de uma mesma réplica.
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * Acedido concorrentemente por múltiplas threads:
 * 1. Threads de Entrada do WebSocket (PubSubHub): Invocam onHeartbeat() e registarOuAtualizar();
 * 2. Thread do Agendador Periódico (LeaderBot scheduler): Invoca atualizarEstados() a cada 1 s;
 * 3. Thread de Selagem e Distribuição de Chunks: Invoca reserveAndChoose(), confirmarReserva() e libertar();
 * 4. Threads de Slash Commands do Discord (JDA): Invocam snapshot() para /estado e removerReplica() para /replica-remover.
 * 
 * HIERARQUIA DE SINCRONIZAÇÃO (Prevenção de Race Conditions):
 * Todos os métodos que leem ou alteram o mapa de réplicas e as quotas são 'synchronized(this)'.
 * Em particular, 'reserveAndChoose' realiza a verificação de espaço livre, o cálculo do Placement
 * e a soma imediata dos bytes reservados num único passo indivisível sob o mesmo monitor.
 * 
 * ONDE É UTILIZADA:
 * Instanciada no construtor do LeaderBot e partilhada com o PubSubHub e o agendador de tarefas.
 */
public final class ReplicaRegistry {

    /**
     * Estrutura de dados que armazena a informação dinâmica de uma réplica individual.
     */
    public static final class InfoReplica {
        public final String id;
        public final HeartbeatMonitor monitor;
        public long quota;       // Capacidade total em bytes declarada pela réplica no HB
        public long usados;      // Bytes efetivamente ocupados por ficheiros .bin na réplica
        public long reservados;  // Bytes provisoriamente bloqueados por envios STORE pendentes de ACK
        public boolean removida; // Marcação de réplica revogada administrativamente

        public InfoReplica(String id, long agoraMs) {
            this.id = id;
            this.monitor = new HeartbeatMonitor(agoraMs);
        }

        /**
         * Calcula o espaço em disco que ainda pode ser alocado com segurança.
         * Garante que não devolve valores negativos mesmo perante oscilações de rede.
         */
        public long livreEfetivo() {
            return Math.max(0L, quota - usados - reservados);
        }
    }

    private final Map<String, InfoReplica> replicas = new LinkedHashMap<>();
    private final Placement placement = new Placement(Wire.ALFA, Wire.MARGEM_BYTES);
    private final BiConsumer<String, String> eventLogger;

    /**
     * Inicializa o registo com um consumidor para gravação de eventos operacionais.
     *
     * @param eventLogger Callback que escreve em eventos-lider.txt (evento, detalhe)
     */
    public ReplicaRegistry(BiConsumer<String, String> eventLogger) {
        this.eventLogger = eventLogger;
    }

    /**
     * Regista uma nova réplica ou repõe como ativa após autenticação no hub.
     * 
     * Onde é utilizado: PubSubHub ao validar com sucesso a PROVA Ed25519 de uma réplica.
     */
    public synchronized void registarOuAtualizar(String id) {
        replicas.computeIfAbsent(id, k -> new InfoReplica(k, System.currentTimeMillis())).removida = false;
    }

    /**
     * Marca uma réplica como excluída administrativamente e zera as suas reservas pendentes.
     * 
     * Onde é utilizado: LeaderBot ao processar o slash command /replica-remover.
     */
    public synchronized void removerReplica(String id) {
        InfoReplica info = replicas.get(id);
        if (info != null) {
            info.removida = true;
            info.reservados = 0;
        }
    }

    /**
     * Atualiza a quota e os bytes usados reportados pelo Heartbeat periódico da réplica,
     * transitando o seu estado para VIVA e registando o evento se houver recuperação.
     * 
     * ou seja se antigamente o estado era morto ou suspeita, para diretamente para viva porque entregou o hb
     * 
     * Onde é utilizado: LeaderBot.processarMensagemLider() ao receber 'HB;<id>;<usados>;<quota>'.
     */
    public synchronized void onHeartbeat(String id, long usados, long quota) {
        long agora = System.currentTimeMillis();
        InfoReplica info = replicas.computeIfAbsent(id, k -> new InfoReplica(k, agora));
        if (info.removida) return;

        HeartbeatMonitor.Estado antigo = info.monitor.getEstado();
        info.monitor.registarHeartbeat(agora);
        info.usados = usados;
        info.quota = quota;

        if (antigo != HeartbeatMonitor.Estado.VIVA) {
            eventLogger.accept("ESTADO", id + " " + antigo + " -> VIVA");//grava no ficheiro de eventos do lider
        }
    }

    /**
     * Percorre periodicamente todas as réplicas ativas e avalia transições de tempo (SUSPEITA/MORTA).
     * 
     * Onde é utilizado: Tarefa periódica agendada no LeaderBot a cada 1000 ms.
     * verifica a cada 1 segundo se o hb ja atrasou e o estado passou de viva para suspeita (6 s), 
     * ou de suspeita para morta (10 s)
     */
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
     * Seleciona até 'count' réplicas elegíveis e reserva imediatamente o tamanho do chunk,
     * tudo dentro da mesma secção crítica sincronizada.
     * 
     * Por que a reserva é atómica com a escolha?
     * Sem esta proteção, se dois chunks selados quase ao mesmo tempo tentassem escolher réplicas,
     * ambas as threads veriam o mesmo espaço livre e reservariam a mesma máquina duas vezes,
     * estourando a quota física da réplica.
     * 
     * serve para ver aquelas que estao vivas e depois escolher as R replicas
     *
     * @param chunkId Identificador SHA-256 do chunk a colocar
     * @param tamanho Tamanho total em bytes do ficheiro .bin cifrado
     * @param count Número de réplicas pretendidas (normalmente R = 3)
     * @param exclude Conjunto de identificadores de réplicas a não selecionar (ex: réplicas já tentadas)
     * @return Lista com os identificadores das réplicas escolhidas e com reservas atribuídas
     */
    public synchronized List<String> reserveAndChoose(String chunkId, long tamanho, int count, Set<String> exclude) {
        List<Placement.Candidate> candidatos = new ArrayList<>();
        for (InfoReplica info : replicas.values()) {
            if (info.removida) continue;
            // Exclui réplicas cujo monitor indica estado MORTA
            if (info.monitor.getEstado() != HeartbeatMonitor.Estado.MORTA) {
                candidatos.add(new Placement.Candidate(info.id, info.livreEfetivo()));
            }
        }

        List<String> escolhidas = placement.choose(chunkId, tamanho, count, candidatos, exclude);
        for (String id : escolhidas) {//percorre as x escolhidas e reserva o espaço (ou seja aumenta o espaço e fica como reservada, so passa a usada quando entregar o ACK)
            InfoReplica info = replicas.get(id);
            if (info != null) {
                info.reservados += tamanho;
            }
        }
        return escolhidas;
    }

    /**
     * Converte o espaço reservado em espaço usado após a receção de um ACK válido da réplica.
     * Impede que o espaço livre fique sobrestimado antes da chegada do próximo Heartbeat regular.
     * 
     * Onde é utilizado: LeaderBot.processarMensagemLider() ao validar um ACK.
     */
    public synchronized void confirmarReserva(String replicaId, long tamanho) {
        InfoReplica info = replicas.get(replicaId);
        if (info != null) {
            info.reservados = Math.max(0L, info.reservados - tamanho);//liberta o espaço reservado
            info.usados += tamanho;//aumenta o espaço usado
        }
    }

    /**
     * Liberta o espaço reservado caso a réplica não confirme o ACK dentro de ACK_TIMEOUT_MS.
     * 
     * Onde é utilizado: LeaderBot.agendarNovaRonda().
     */
    public synchronized void libertar(String replicaId, long tamanho) {
        InfoReplica info = replicas.get(replicaId);
        if (info != null) {
            info.reservados = Math.max(0L, info.reservados - tamanho);
        }
    }

    /**
     * Devolve uma cópia segura (snapshot) da lista de réplicas ativas para visualização.
     * 
     * Onde é utilizado: LeaderBot ao compor a resposta do slash command /estado.
     */
    public synchronized List<InfoReplica> snapshot() {
        List<InfoReplica> copy = new ArrayList<>();
        for (InfoReplica r : replicas.values()) {
            if (!r.removida) copy.add(r);
        }
        return copy;
    }
}
