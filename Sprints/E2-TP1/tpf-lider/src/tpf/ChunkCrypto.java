package tpf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Motor Criptográfico do Líder (Zero-Knowledge, Cifra Autenticada e HKDF).
 * 
 * PAPEL NO SISTEMA:
 * - Garante confidencialidade e integridade ponta-a-ponta (Zero-Knowledge).
 * - Apenas o Líder detém a 'master.key' de 32 bytes (256 bits). As réplicas voluntárias
 *   nunca têm acesso à chave mestra, manipulando apenas blocos opacos (.bin e .hdr).
 * - Para cada chunk gerado:
 *   1. Gera um salt aleatório (32 bytes) e deriva uma Data Encryption Key (DEK) via HKDF-SHA256 (RFC 5869).
 *   2. Gera um nonce/IV aleatório (12 bytes) e cifra o bloco com AES-256-GCM autenticado.
 *   3. Associa Dados Autenticados Adicionais (AAD) incluindo a versão da chave, salt e nonce,
 *      impedindo ataques de reutilização ou troca de metadados.
 *   4. Atribui o identificador 'chunkId' pelo hash SHA-256 do texto cifrado gerado (endereçamento por conteúdo).
 * 
 * MODELO DE THREADS E CONCORRÊNCIA:
 * - Os métodos seal() e open() instanciam os seus próprios objetos locais de Mac e Cipher
 *   (que não são thread-safe por natureza do JDK), tornando a instância de ChunkCrypto
 *   totalmente thread-safe e reentrante para múltiplas chamadas concorrentes.
 * - SecureRandom (RNG) é nativamente thread-safe.
 * 
 * ONDE É UTILIZADA:
 * - LeaderBot.selarChunkAtual(): cifra o lote de registos serializado antes da persistência local e difusão.
 * - ChunkCryptoTest: bateria exaustiva de testes de round-trip e corrupção deliberada.
 */
public final class ChunkCrypto {

    /**
     * Estrutura que encapsula um chunk selado pronto a ser enviado às réplicas.
     * Nenhum registo em claro sai do líder nesta estrutura.
     */
    public static final class Sealed {
        public final String id;          // SHA-256 do texto cifrado em hexadecimal (chunkId)
        public final int keyVersion;     // Versão da master key utilizada (ex: 1)
        public final byte[] salt;        // Salt aleatório de 32 bytes usado no HKDF
        public final byte[] nonce;       // Vetor de inicialização de 12 bytes do AES-GCM
        public final byte[] ciphertext;  // Texto cifrado (incluindo tag de autenticação GCM de 16 bytes)

        public Sealed(String id, int keyVersion, byte[] salt, byte[] nonce, byte[] ciphertext) {
            this.id = id;
            this.keyVersion = keyVersion;
            this.salt = salt;
            this.nonce = nonce;
            this.ciphertext = ciphertext;
        }
    }

    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();

    private final byte[] masterKey;
    private final int keyVersion;

    /**
     * Inicializa a instância com a master key secreta de 32 bytes e a versão da chave.
     *
     * @param masterKey Array de 32 bytes lido de master.key
     * @param keyVersion Versão numérica da chave (ex: 1)
     */
    public ChunkCrypto(byte[] masterKey, int keyVersion) {
        if (masterKey.length != 32) throw new IllegalArgumentException("a master key deve ter 32 bytes");
        this.masterKey = masterKey.clone();
        this.keyVersion = keyVersion;
    }

    /**
     * Derivação de chave HKDF-SHA256 (RFC 5869) com saída fixa de 32 bytes (DEK).
     * Fase de Extract (HMAC com salt) seguida de Expand (bloco de 32 bytes com INFO).
     * 
     * Onde é utilizado: seal() e open() internamente.
     */
    static byte[] hkdf(byte[] ikm, byte[] salt, byte[] info) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(ikm);
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(info);
        mac.update((byte) 0x01);
        return mac.doFinal();
    }

    /**
     * Constrói os dados associados autenticados (AAD) protegidos pela tag GCM:
     * AAD = "tpf-v1" || keyVersion (4 bytes) || salt (32 bytes) || nonce (12 bytes).
     * Impede qualquer tentativa de adulteração do cabeçalho sem que a tag falhe.
     */
    static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        return ByteBuffer.allocate(6 + 4 + salt.length + nonce.length)
                .put("tpf-v1".getBytes(StandardCharsets.US_ASCII))
                .putInt(keyVersion).put(salt).put(nonce).array();
    }

    /**
     * Calcula o hash SHA-256 em formato hexadecimal de um array de bytes.
     * 
     * Onde é utilizado: cálculo de chunkId e validação de ficheiros binários.
     */
    public static String sha256Hex(byte[] data) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    /**
     * Sela e cifra um bloco de bytes em claro.
     * Gera salt e nonce aleatórios, deriva a DEK e cifra em AES-256-GCM com AAD.
     * 
     * Onde é utilizado: LeaderBot.selarChunkAtual().
     *
     * @param plaintext Bytes com padding serializados de Chunk.serialize()
     * @return Estrutura Sealed com o identificador de conteúdo e campos cifrados
     */
    public Sealed seal(byte[] plaintext) throws GeneralSecurityException {
        byte[] salt = new byte[32];
        byte[] nonce = new byte[12];
        RNG.nextBytes(salt);
        RNG.nextBytes(nonce);
        byte[] dek = hkdf(masterKey, salt, INFO);
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
        c.updateAAD(aad(keyVersion, salt, nonce));
        byte[] ct = c.doFinal(plaintext);
        return new Sealed(sha256Hex(ct), keyVersion, salt, nonce, ct);
    }

    /**
     * Decifra e valida a integridade de um chunk selado.
     * Lança exceção se:
     * - O hash do texto cifrado for diferente do chunkId pretendido;
     * - A versão da chave não for suportada;
     * - Qualquer byte do texto cifrado, salt, nonce ou chave mestra tiver sido adulterado (AEADBadTagException).
     * 
     * Onde é utilizado: ChunkCryptoTest e leitura para recuperação no Sprint 4.
     */
    public byte[] open(Sealed s) throws GeneralSecurityException {
        if (!sha256Hex(s.ciphertext).equals(s.id))
            throw new GeneralSecurityException("INTEGRIDADE: o hash difere de " + s.id);
        if (s.keyVersion != keyVersion)
            throw new GeneralSecurityException("versao de chave desconhecida: " + s.keyVersion);
        byte[] dek = hkdf(masterKey, s.salt, INFO);
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, s.nonce));
        c.updateAAD(aad(s.keyVersion, s.salt, s.nonce));
        return c.doFinal(s.ciphertext);   // Lança AEADBadTagException se adulterado
    }
}
