package tpf;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Algoritmo de Colocação Ponderada de Chunks em Réplicas (Rendezvous Hashing / HRW).
 * 
 * PAPEL NO SISTEMA:
 * Substitui o hashing estático e a replicação integral ingénua das fichas anteriores.
 * Permite selecionar as melhores réplicas voluntárias para cada chunk com base em:
 * 1. Espaço Livre Disponível: Evita sobrecarregar máquinas com discos pequenos ou cheios;
 * 2. Pseudo-Aleatoriedade Determinística: O mesmo chunk com o mesmo estado do cluster produz
 *    sempre a mesma ordenação exata de réplicas sem necessidade de coordenação central complexa;
 * 3. Parâmetro de Ponderação Alfa (ALFA):
 *    - alfa = 0.0: Puramente uniforme (cada réplica tem probabilidade igual 1/N, independentemente do espaço);
 *    - alfa = 1.0: Proporcional ao espaço livre (uma réplica com o dobro do espaço recebe o dobro dos chunks);
 *    - alfa = 2.0: Ponderação agressiva em favor de máquinas com grande capacidade.
 * 
 * FÓRMULA DE PONTUAÇÃO:
 *   score = (livre^alfa) / ( -ln( u(chunkId, replicaId) ) )
 * Onde:
 *   - 'livre' é o espaço em bytes livre efetivo (quota - usados - reservados);
 *     usados sao os ficheiros bin/hdr que ja estao na pasta de dados da replica, reservados sao os espacos que foram reservados para os ficheiros bin/hdr que ainda nao foram escritos
 *   - 'u' é um valor pseudo-aleatório uniforme no intervalo aberto ]0, 1[, gerado a partir
 *     dos primeiros 53 bits da hash SHA-256 da concatenação "chunkId|replicaId".
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - Imutável (campos alpha e margin são final).
 * - Totalmente thread-safe e sem efeitos secundários; pode ser executado em paralelo.
 * 
 * ONDE É UTILIZADA:
 * - ReplicaRegistry.reserveAndChoose(): seleção de réplicas alvo no momento da selagem.
 * - PlacementExperiment: validação estatística da distribuição em 10 000 chunks.
 */
public final class Placement {

    /**
     * Vista de uma réplica candidata no instante exato da tomada de decisão.
     */
    public static final class Candidate {
        public final String id;
        public final long free; // Espaço livre efetivo em bytes (quota - usados - reservados)

        public Candidate(String id, long free) {
            this.id = id;
            this.free = free;
        }
    }

    private final double alpha; // ponderacao do espaco livre (como temos igual a 1.0, a escolha é proporcional ao espaco livre)
    private final long margin;// margem de seguranca (normalmente igual a 64 kb), serve para evitar que os ficheiros fiquem muito perto do limite da quota

    /**
     * Construtor da estratégia de colocação.
     *
     * @param alpha Expoente de ponderação de espaço livre (normalmente Wire.ALFA = 1.0)
     * @param margin Margem de segurança mínima adicional em bytes (normalmente Wire.MARGEM_BYTES = 64 KB)
     */
    public Placement(double alpha, long margin) {
        this.alpha = alpha;
        this.margin = margin;
    }

    /**
     * Gera um valor uniforme pseudo-aleatório no intervalo aberto ]0, 1[ a partir
     * dos primeiros 53 bits (precisão de double IEEE 754) de SHA-256(chunkId + "|" + replicaId).
     * 
     * Serve para gerar um valor pseudo-aleatório entre 0 e 1 para o algoritmo de ponderação de espaço livre.
     * 
     * Onde é utilizado: Placement.score().
     */
    static double unit(String chunkId, String replicaId) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256")
                    .digest((chunkId + "|" + replicaId).getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 8; i++) v = (v << 8) | (h[i] & 0xff);
            v >>>= 11; // 64 - 11 = 53 bits úteis
            return (v + 0.5) / (double) (1L << 53); // Garante ]0, 1[ sem divisão por zero
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Calcula a pontuação da réplica candidata para o chunk especificado.
     * 
     * Onde é utilizado: Placement.choose() para ordenação.
     */
    double score(String chunkId, Candidate c) {
        return Math.pow((double) c.free, alpha) / -Math.log(unit(chunkId, c.id));
        // math pow é só para elevar à potencia de alpha o free, se for free=10 e alpha=2, entao o resultado é 10^2=100
        // math log é o logaritmo natural, que é o logaritmo na base e, neste caso -math log é para gerar um valor entre 0 e 1, quanto maior o valor, menor o score, quanto menor o valor, maior o score
    }

    /**
     * Escolhe até 'count' réplicas elegíveis para alojar o chunk, ordenadas por pontuação decrescente.
     * Descarta réplicas que:
     * 1. Estejam no conjunto de exclusão 'exclude' (ex: réplicas já tentadas em rondas anteriores);
     * 2. Não tenham espaço suficiente para o chunk mais a margem de segurança (free < size + margin).
     * 
     * Onde é utilizado: ReplicaRegistry.reserveAndChoose().
     *
     * @param chunkId Identificador SHA-256 do chunk
     * @param size Tamanho em bytes do chunk cifrado
     * @param count Número máximo de réplicas pretendidas (normalmente R = 3)
     * @param alive Lista de réplicas ativas candidatas (podem ser replicas suspeitas tambem, se houver uma suspeita e uma viva, mas a suspeita tem mais espaço, a suspeita pode ser selecionada)
     * @param exclude Conjunto de réplicas a não selecionar
     * @return Lista ordenada com os identificadores das réplicas selecionadas
     */
    public List<String> choose(String chunkId, long size, int count,
                               Collection<Candidate> alive, Set<String> exclude) {
        List<Candidate> ok = new ArrayList<>();
        for (Candidate c : alive) {
            if (!exclude.contains(c.id) && c.free >= size + margin) {
                ok.add(c);
            }
        }
        ok.sort(Comparator.comparingDouble((Candidate c) -> score(chunkId, c)).reversed());//ordenacao decrescente
        //todas as que estao ok, vai ordenar pelo score, selecionando as que tiverem maior score (se o fator é 3, seleciona as 3 melhores)
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(count, ok.size()); i++) {
            out.add(ok.get(i).id);
        }
        return out;
    }
}
