package tpf;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

/**
 * Suite de Testes Criptográficos Unitários e Validação de Integridade do Arquivo.
 * 
 * PAPEL NO SISTEMA:
 * Valida de forma automatizada todos os requisitos criptográficos obrigatórios do enunciado:
 * 1. Teste de Roundtrip: Cifrar um conjunto de registos, decifrá-los e confirmar que são idênticos.
 * 2. Testes de Resiliência e Adulteração: Garante que a decifra (open) falha com exceção segura
 *    se qualquer um dos seguintes elementos for adulterado:
 *    - Um único byte do ficheiro .bin (texto cifrado);
 *    - O valor do salt guardado no .hdr;
 *    - O valor do nonce guardado no .hdr;
 *    - A versão da chave (keyVersion);
 *    - A própria master key do Líder (tentativa com chave diferente).
 * 3. Validação de Ficheiros Reais: Percorre a pasta local 'selados/' e decifra todos os blocos
 *    físicos gerados pelo LeaderBot durante as execuções.
 * 
 * MODELO DE THREADS:
 * Execução sequencial na Thread Main (arranque direto em linha de comandos).
 * 
 * COMO EXECUTAR:
 * java -cp "out;lib/*" tpf.ChunkCryptoTest
 */
public class ChunkCryptoTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== TESTES OBRIGATÓRIOS DE ChunkCrypto E Chunk ===");

        SecureRandom rng = new SecureRandom();
        byte[] mk1 = new byte[32];
        byte[] mk2 = new byte[32];
        rng.nextBytes(mk1);
        rng.nextBytes(mk2);

        ChunkCrypto crypto1 = new ChunkCrypto(mk1, 1);
        ChunkCrypto cryptoOtherKey = new ChunkCrypto(mk2, 1);
        ChunkCrypto cryptoOtherVersion = new ChunkCrypto(mk1, 2);

        String b64_1 = Base64.getEncoder().encodeToString("Mensagem secreta 1".getBytes(StandardCharsets.UTF_8));
        String b64_2 = Base64.getEncoder().encodeToString("Mensagem secreta 2".getBytes(StandardCharsets.UTF_8));

        List<Chunk.RecordEntry> originais = List.of(
                new Chunk.RecordEntry(1, 1700000001000L, "andre", "arquivo", b64_1),
                new Chunk.RecordEntry(2, 1700000002000L, "gabriel", "arquivo", b64_2)
        );

        // 1. Cifrar e decifrar com sucesso
        byte[] padded = Chunk.serialize(originais, Wire.PAD_BYTES);
        ChunkCrypto.Sealed sealed = crypto1.seal(padded);
        byte[] opened = crypto1.open(sealed);
        List<Chunk.RecordEntry> recuperados = Chunk.deserialize(opened);
        if (!originais.equals(recuperados)) throw new AssertionError("Os registos recuperados diferem!");
        System.out.println("[OK] 1. Cifrar e decifrar com sucesso.");

        // 2. Byte alterado no texto cifrado -> exceção
        byte[] badCt = sealed.ciphertext.clone();
        badCt[0] ^= 0x01;
        expectFail(() -> crypto1.open(new ChunkCrypto.Sealed(sealed.id, sealed.keyVersion, sealed.salt, sealed.nonce, badCt)),
                "Byte alterado no texto cifrado");

        // 3. Salt diferente -> exceção
        byte[] badSalt = sealed.salt.clone();
        badSalt[0] ^= 0xFF;
        expectFail(() -> crypto1.open(new ChunkCrypto.Sealed(sealed.id, sealed.keyVersion, badSalt, sealed.nonce, sealed.ciphertext)),
                "Salt diferente");

        // 4. Nonce diferente -> exceção
        byte[] badNonce = sealed.nonce.clone();
        badNonce[0] ^= 0xFF;
        expectFail(() -> crypto1.open(new ChunkCrypto.Sealed(sealed.id, sealed.keyVersion, sealed.salt, badNonce, sealed.ciphertext)),
                "Nonce diferente");

        // 5. keyVersion diferente -> exceção
        expectFail(() -> cryptoOtherVersion.open(sealed), "keyVersion diferente");

        // 6. master.key diferente -> exceção
        expectFail(() -> cryptoOtherKey.open(sealed), "master.key diferente");

        // 7. Valida os chunks reais gravados em selados/
        Path baseDir = Files.isDirectory(Paths.get("Sprints/E2-TP1/tpf-lider"))
                ? Paths.get("Sprints/E2-TP1/tpf-lider")
                : (Files.isDirectory(Paths.get("Sprints/E2-TP2/tpf-lider"))
                        ? Paths.get("Sprints/E2-TP2/tpf-lider") : Paths.get("."));
        Path mkPath = baseDir.resolve("master.key");
        Path seladosDir = baseDir.resolve("selados");

        if (Files.exists(mkPath) && Files.isDirectory(seladosDir)) {
            System.out.println("\n--- A abrir chunks reais de selados/ ---");
            ChunkCrypto realCrypto = new ChunkCrypto(Files.readAllBytes(mkPath), 1);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(seladosDir, "*.bin")) {
                for (Path binFile : stream) {
                    String id = binFile.getFileName().toString().replace(".bin", "");
                    Path hdrFile = seladosDir.resolve(id + ".hdr");
                    byte[] ct = Files.readAllBytes(binFile);
                    String[] hdr = Files.readString(hdrFile, StandardCharsets.UTF_8).trim().split(";");
                    ChunkCrypto.Sealed s = new ChunkCrypto.Sealed(
                            id,
                            Integer.parseInt(hdr[0]),
                            Base64.getDecoder().decode(hdr[1]),
                            Base64.getDecoder().decode(hdr[2]),
                            ct
                    );
                    List<Chunk.RecordEntry> entries = Chunk.deserialize(realCrypto.open(s));
                    System.out.println("Chunk " + id.substring(0, 12) + "... -> " + entries.size() +
                            " registos (LSN " + entries.get(0).lsn + ".." + entries.get(entries.size() - 1).lsn + ")");
                }
            }
        }
        System.out.println("\n=== TODOS OS TESTES PASSARAM ===");
    }

    @FunctionalInterface
    interface Check { void run() throws Exception; }

    private static void expectFail(Check c, String label) {
        try {
            c.run();
            throw new AssertionError("Deveria ter falhado: " + label);
        } catch (GeneralSecurityException ok) {
            System.out.println("[OK] Exceção lançada para: " + label);
        } catch (Exception e) {
            throw new AssertionError("Exceção inesperada em " + label + ": " + e);
        }
    }
}
