package tpf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Catálogo Persistente e Autoritativo de Chunks do Líder (catalogo.txt).
 * 
 * PAPEL NO SISTEMA:
 * Mantém o índice de metadados de todos os chunks gerados pelo Líder no sistema distribuído:
 * - Mapeia cada 'chunkId' ao intervalo de registos contidos [lsnDe..lsnAte] e tamanho em bytes;
 * - Regista a lista de réplicas que já confirmaram formalmente ter uma cópia do bloco (via ACK);
 * - Gere a transição de estado de cada chunk:
 *   * SELADO: Bloco cifrado e guardado localmente, mas com menos cópias do que o quórum de durabilidade;
 *   * DURAVEL: Bloco confirmado por pelo menos quorumDuravel(R) réplicas (maioria e >= W_MIN);
 * - Determina a condição de 'subreplicado': quando possui menos de R cópias confirmadas
 *   (um chunk pode ser DURAVEL e subreplicado simultaneamente, ex: 2 cópias com R=3).
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - Acedido concorrentemente por:
 *   1. Thread de Selagem do Líder: Invoca adicionar() ao fechar um chunk aberto;
 *   2. Threads de Entrada do WebSocket (PubSubHub): Invocam confirmar() ao processar mensagens ACK;
 *   3. Threads de Slash Commands (JDA): Invocam todas() para construir a resposta do comando /estado;
 *   4. Threads de Recuperação (Sprint 4): Invocam copias() e get().
 * - Sincronização via 'synchronized(this)' em todos os métodos que manipulam o mapa de entradas.
 * 
 * ESCRITA ATÓMICA NO DISCO (Crash-Consistency):
 * Cada alteração ao catálogo regrava o ficheiro completo num temporário ('catalogo.txt.tmp')
 * e executa uma substituição atómica via 'Files.move(..., ATOMIC_MOVE)'. Isto garante que
 * em caso de quebra de energia ou falha do processo do Líder, o catálogo nunca fica corrompido.
 * 
 * FORMATO DE catalogo.txt:
 * chunkId;lsnDe;lsnAte;tamanho;replicasSeparadasPorVirgula;estado
 * 
 * ONDE É UTILIZADA:
 * Instanciada no construtor do LeaderBot.
 */
public final class Catalog {

    /**
     * Estados de ciclo de vida de um chunk no catálogo.
     */
    public enum EstadoChunk {
        SELADO, DURAVEL
    }

    /**
     * Entrada individual representativa de um chunk no catálogo.
     */
    public static final class EntradaCatalogo {
        public final String chunkId;
        public final long lsnDe;
        public final long lsnAte;
        public final long tamanho;
        public final Set<String> replicas = new LinkedHashSet<>();
        public EstadoChunk estado;

        public EntradaCatalogo(String chunkId, long lsnDe, long lsnAte, long tamanho, EstadoChunk estado) {
            this.chunkId = chunkId;
            this.lsnDe = lsnDe;
            this.lsnAte = lsnAte;
            this.tamanho = tamanho;
            this.estado = estado;
        }

        /**
         * Serializa a entrada numa linha delimitada por ponto e vírgula:
         * "chunkId;lsnDe;lsnAte;tamanho;rep1,rep2;DURAVEL"
         */
        public String toLine() {
            String reps = String.join(",", replicas);
            return chunkId + ";" + lsnDe + ";" + lsnAte + ";" + tamanho + ";" + reps + ";" + estado.name();
        }
    }

    private final Path catalogPath;
    private final Map<String, EntradaCatalogo> entradas = new LinkedHashMap<>();

    /**
     * Inicializa o catálogo localizando o ficheiro 'catalogo.txt' na pasta base e carregando
     * as entradas existentes do disco no arranque do Líder.
     */
    public Catalog(Path baseDir) {
        this.catalogPath = baseDir.resolve("catalogo.txt");
        carregar();
    }

    /**
     * Lê e reconstrói o catálogo em memória a partir de 'catalogo.txt' no arranque.
     */
    private synchronized void carregar() {
        if (!Files.exists(catalogPath)) return;
        try {
            List<String> lines = Files.readAllLines(catalogPath, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isBlank()) continue;
                String[] parts = line.split(";", 6);
                if (parts.length == 6) {
                    String id = parts[0];
                    long lsnDe = Long.parseLong(parts[1]);
                    long lsnAte = Long.parseLong(parts[2]);
                    long tamanho = Long.parseLong(parts[3]);
                    EstadoChunk est = EstadoChunk.valueOf(parts[5]);
                    EntradaCatalogo e = new EntradaCatalogo(id, lsnDe, lsnAte, tamanho, est);
                    if (!parts[4].isBlank()) {
                        for (String r : parts[4].split(",")) {
                            if (!r.isBlank()) e.replicas.add(r.trim());
                        }
                    }
                    entradas.put(id, e);
                }
            }
            System.out.println("Catálogo carregado com " + entradas.size() + " entradas.");
        } catch (Exception e) {
            System.err.println("Erro ao carregar catalogo.txt: " + e.getMessage());
        }
    }

    /**
     * Regista um novo chunk recém-selado com estado inicial SELADO e persiste atomicamente.
     * 
     * Onde é utilizado: LeaderBot.selarChunkAtual().
     */
    public synchronized void adicionar(String chunkId, long lsnDe, long lsnAte, long tamanho) {
        EntradaCatalogo e = new EntradaCatalogo(chunkId, lsnDe, lsnAte, tamanho, EstadoChunk.SELADO);
        entradas.put(chunkId, e);
        gravarAtomico();
    }

    /**
     * Regista a confirmação formal (ACK) recebida de uma réplica para um chunk:
     * - Adiciona a réplica ao conjunto de detentores da cópia;
     * - Avalia se atingiu o quórum de durabilidade: replicas.size() >= Wire.quorumDuravel(Wire.R);
     * - Se atingir o quórum, transita o estado de SELADO para DURAVEL;
     * - Regrava atomicamente o catálogo em disco.
     * 
     * Onde é utilizado: LeaderBot.processarMensagemLider() ao validar um ACK.
     *
     * @param chunkId Identificador do chunk confirmado
     * @param replicaId Identificador da réplica que enviou o ACK
     * @return true se esta confirmação específica causou a transição para DURAVEL
     */
    public synchronized boolean confirmar(String chunkId, String replicaId) {
        EntradaCatalogo e = entradas.get(chunkId);
        if (e == null) return false;

        boolean mudou = e.replicas.add(replicaId);
        boolean tornouDuravel = false;

        int quorum = Wire.quorumDuravel(Wire.R);
        if (e.estado == EstadoChunk.SELADO && e.replicas.size() >= quorum) {
            e.estado = EstadoChunk.DURAVEL;
            tornouDuravel = true;
            mudou = true;
        }

        if (mudou) {
            gravarAtomico();
        }
        return tornouDuravel;
    }

    /**
     * Devolve o conjunto de IDs de réplicas que possuem cópias confirmadas do chunk.
     */
    public synchronized Set<String> copias(String chunkId) {
        EntradaCatalogo e = entradas.get(chunkId);
        return e == null ? Collections.emptySet() : new LinkedHashSet<>(e.replicas);
    }

    /**
     * Obtém uma entrada de catálogo por chunkId.
     */
    public synchronized EntradaCatalogo get(String chunkId) {
        return entradas.get(chunkId);
    }

    /**
     * Devolve uma cópia de todas as entradas existentes no catálogo para inspeção.
     * 
     * Onde é utilizado: LeaderBot (/estado).
     */
    public synchronized Collection<EntradaCatalogo> todas() {
        return new ArrayList<>(entradas.values());
    }

    /**
     * Gravação atómica de 'catalogo.txt' no disco (Temporary File + ATOMIC_MOVE).
     */
    public synchronized void gravarAtomico() {
        try {
            StringBuilder sb = new StringBuilder();
            for (EntradaCatalogo e : entradas.values()) {
                sb.append(e.toLine()).append("\n");
            }
            Path tmp = catalogPath.resolveSibling("catalogo.txt.tmp");
            Files.write(tmp, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, catalogPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Erro ao gravar catalogo.txt atomicamente: " + e.getMessage());
        }
    }
}
