package tpf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Catálogo persistente do Líder (catalogo.txt).
 * Formato por linha: chunkId;lsnDe;lsnAte;tamanho;replicas;estado
 */
public final class Catalog {

    public enum EstadoChunk {
        SELADO, DURAVEL
    }

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

        public String toLine() {
            String reps = String.join(",", replicas);
            return chunkId + ";" + lsnDe + ";" + lsnAte + ";" + tamanho + ";" + reps + ";" + estado.name();
        }
    }

    private final Path catalogPath;
    private final Map<String, EntradaCatalogo> entradas = new LinkedHashMap<>();

    public Catalog(Path baseDir) {
        this.catalogPath = baseDir.resolve("catalogo.txt");
        carregar();
    }

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

    public synchronized void adicionar(String chunkId, long lsnDe, long lsnAte, long tamanho) {
        EntradaCatalogo e = new EntradaCatalogo(chunkId, lsnDe, lsnAte, tamanho, EstadoChunk.SELADO);
        entradas.put(chunkId, e);
        gravarAtomico();
    }

    /**
     * Regista a confirmação (ACK) de uma réplica para um chunk.
     * Devolve true se esta confirmação fez o chunk transitar para DURAVEL agora.
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

    public synchronized Set<String> copias(String chunkId) {
        EntradaCatalogo e = entradas.get(chunkId);
        return e == null ? Collections.emptySet() : new LinkedHashSet<>(e.replicas);
    }

    public synchronized EntradaCatalogo get(String chunkId) {
        return entradas.get(chunkId);
    }

    public synchronized Collection<EntradaCatalogo> todas() {
        return new ArrayList<>(entradas.values());
    }

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