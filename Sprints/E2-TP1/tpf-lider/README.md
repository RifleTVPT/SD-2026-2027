# LeaderBot — Líder do Arquivo Distribuído (TPF)

Este componente é o ponto de entrada do sistema distribuído e gere a integração com o Discord através de Slash Commands e da monitorização do canal `#arquivo`.

---

## 1. Configuração do Token do Discord

Por motivos de segurança, o **Token do Bot** NUNCA deve ser guardado no repositório Git.

O `LeaderBot` aceita o token através de qualquer uma das seguintes opções (por ordem de prioridade):

1. **Ficheiro local `token.txt` (Recomendado para desenvolvimento local):**
   Cria um ficheiro `token.txt` nesta pasta (`Sprints/E2-TP1/tpf-lider/token.txt`) ou na raiz do repositório contendo apenas o teu token:
   ```text
   MTMyNDU2Nzg5MDEyMzQ1Njc4OQ...
   ```
   *(Este ficheiro está no `.gitignore` e não será enviado para o GitHub).*

2. **Variável de Ambiente:**
   No PowerShell:
   ```powershell
   $env:DISCORD_TOKEN="O_TEU_TOKEN_AQUI"
   ```

3. **Argumento de Linha de Comandos:**
   ```powershell
   java -cp "out;lib/*" tpf.LeaderBot O_TEU_TOKEN_AQUI
   ```

4. **Propriedade de Sistema:**
   ```powershell
   java -Dtpf.token="O_TEU_TOKEN_AQUI" -cp "out;lib/*" tpf.LeaderBot
   ```

---

## 2. Compilação e Execução

A partir da raiz do repositório:

### Compilar:
```powershell
javac -encoding UTF-8 -cp "Sprints/E2-TP1/tpf-lider/lib/*" -d Sprints/E2-TP1/tpf-lider/out Sprints/E2-TP1/tpf-lider/src/tpf/*.java
```

### Executar:
```powershell
java -cp "Sprints/E2-TP1/tpf-lider/out;Sprints/E2-TP1/tpf-lider/lib/*" tpf.LeaderBot
```

*(Nota: Para poupar o plafond da API do Discord — limite de 200 registos/dia —, os comandos ficam guardados em cache no Discord após a primeira execução. Se alterares a lista de comandos e precisares de forçar a atualização, adiciona a flag `-Dtpf.registarComandos=true`).*

---

## 3. Comandos Suportados no Discord

* **Gerais:**
  * `/registar <texto>`: Regista uma linha de texto.
  * `/estado`: Mostra o estado do líder e réplicas.
  * `/recuperar <de> <ate>`: Pede a recuperação de um intervalo de LSN.
* **Administração (Requer papel `admin-arquivo`):**
  * `/config r <n>`: Altera o fator de replicação $R$.
  * `/replica-token`: Gera token de uso único para registo de novas réplicas.
  * `/replica-remover <id>`: Desassocia uma réplica.
  * `/rajada <n> <intervaloMs>`: Dispara rajada de teste de carga.

---

## 4. Formato Binário do Chunk e dos Ficheiros Selados

### 4.1 Formato do Chunk em Claro (Serialização e Padding)
* **Serialização de Registos:** Cada registo dentro do bloco é serializado como uma linha de texto no formato:
  ```text
  lsn;instante;autor;canal;textoBase64\n
  ```
* **Padding (Zero-Knowledge):** Para garantir o modelo zero-knowledge e ocultar das réplicas o comprimento real dos registos, o texto serializado em UTF-8 é completado com bytes nulos (`0x00`) até atingir um múltiplo exato de `PAD_BYTES` (4096 bytes).
* **Leitura Inversa:** Na leitura, os bytes nulos finais de preenchimento (`0x00`) são descartados até ao último byte de nova linha (`\n`), reconstruindo fielmente a lista de registos originais.

### 4.2 Cifra Autenticada e Ficheiro `.bin`
* **Derivação de Chave (HKDF-SHA256):** A partir da `master.key` de 32 bytes e de um `salt` aleatório de 32 bytes gerado por chunk, deriva-se a chave DEK de 32 bytes:
  $$\text{DEK} = \text{HKDF-SHA256}(\text{MK}, \text{salt}, \text{info} = \text{"tpf-chunk-v1"})$$
* **Cifra AES-256-GCM:** O bloco com padding é cifrado usando a DEK, um `nonce` aleatório de 12 bytes e dados associados autenticados:
  $$\text{AAD} = \text{"tpf-v1"} \parallel \text{keyVersion (4 bytes)} \parallel \text{salt} \parallel \text{nonce}$$
* **Identificador de Conteúdo (`chunkId`):** O `chunkId` é o hash SHA-256 (em formato hexadecimal) do texto cifrado gerado (incluindo a tag GCM de 16 bytes). O ficheiro é gravado de forma atómica em `selados/<chunkId>.bin`.

### 4.3 Ficheiro de Metadados `.hdr`
* Gravado de forma atómica em `selados/<chunkId>.hdr` com a seguinte linha formatada com `;`:
  ```text
  keyVersion;saltBase64;nonceBase64
  ```
  Onde:
  - `keyVersion`: Versão da chave de cifra (inteiro `1`).
  - `saltBase64`: Salt de 32 bytes codificado em Base64.
  - `nonceBase64`: Vetor de inicialização de 12 bytes codificado em Base64.

---

## 5. Sprint E2-03: Comunicação Líder–Réplicas, Colocação e Escrita Replicada

Componente líder do Arquivo Distribuído Tolerante a Falhas com suporte a replicação parcial, comunicação WebSocket pub/sub, colocação ponderada e tolerância a falhas.

### 5.1 Tópicos e Mensagens Pub/Sub

| Tópico | Publicador | Subscritor |
|---|---|---|
| `tpf-r-<id>` | líder | apenas a réplica `<id>` |
| `tpf-ctl` | líder | todas as réplicas autenticadas |
| `tpf-lider` | réplicas | líder no mesmo processo |

Mensagens trocadas no protocolo (separadas por `;`, binários em Base64):
```text
OLA;<id>;<chavePublicaB64>;<token>
OLA;<id>
DESAFIO;<nonceB64>
PROVA;<id>;<assinaturaB64>
SUB;<topico>
PUB;<topico>;<mensagem>
PUB;tpf-lider;HB;<id>;<usados>;<quota>
PUB;tpf-lider;INVENTARIO;<id>;<chunkId1>,<chunkId2>,...
STORE;<chunkId>;<hdrB64>;<binB64>
PUB;tpf-lider;ACK;<id>;<chunkId>;<sha256>
```

### 5.2 Parâmetros Configuráveis do Sistema

Valores por defeito:
```text
tpf.porto=8765
tpf.periodo=2000
tpf.suspeita=6000
tpf.morta=10000
tpf.tokenValidade=600000
tpf.r=3
tpf.wMin=2
tpf.ackTimeout=4000
tpf.maxRondas=3
tpf.margem=65536
tpf.alfa=1.0
tpf.backoffMin=1000
tpf.backoffMax=30000
tpf.chunkMax=65536
tpf.pad=4096
tpf.selagem=5000
```

A durabilidade usa `max(W_MIN, floor(R/2) + 1)`. Para `R=3` são necessários 2 ACKs; para `R=5` são necessários 3.

### 5.3 Fluxo de Escrita Replicada

1. O líder grava o registo em `registos.log` e chama `fsync`.
2. O temporizador (5 s) ou o limite `CHUNK_MAX_BYTES` (64 KB) sela o chunk.
3. `ChunkCrypto` aplica padding, HKDF-SHA256, AES-256-GCM e calcula o `chunkId`.
4. `Catalog` cria uma entrada `SELADO`.
5. `ReplicaRegistry.reserveAndChoose` reserva espaço de forma atómica e sincronizada.
6. O hub envia `STORE` aos tópicos privados `tpf-r-<id>` escolhidos.
7. Cada réplica grava os ficheiros de forma atómica e responde com `ACK`.
8. O líder valida ID e SHA-256 no ACK, incrementa quórum e passa a entrada a `DURAVEL`.
9. Persistência atómica de `catalogo.txt` com `ATOMIC_MOVE`.

### 5.4 Perguntas de Reflexão do Sprint 3

1. **Por que o token é necessário?**  
   Ed25519 prova posse da chave privada, mas não prova autorização do administrador no cluster. O token liga o primeiro registo a uma autorização efémera (10 min) gerada no Discord por um `admin-arquivo`.
2. **Por que assinar o ID e o prefixo?**  
   A assinatura sobre `"tpf-registo|" + id + "|" + nonceB64` usa separação de domínio e vincula a resposta ao ID da ligação, prevenindo ataques de personificação.
3. **Por que escolha e reserva devem ser sincronizadas?**  
   Sem sincronização, múltiplas threads de selagem poderiam ler o mesmo espaço livre e ultrapassar quotas físicas das réplicas concorrentemente.
4. **Por que durabilidade exige mínimo e maioria?**  
   O quórum `max(W_MIN, floor(R/2) + 1)` impede falsas durabilidades (ex: $W=1$ que perderia dados com uma única falha) e garante maioria tolerante a partições.
5. **Efeito do parâmetro $\alpha$ no Rendezvous Hashing:**  
   - $\alpha=0.0$: Colocação puramente uniforme.
   - $\alpha=1.0$: Colocação proporcionalmente ponderada pelo espaço livre disponível.
   - $\alpha=2.0$: Colocação agressivamente favorável a nós com grande capacidade livre.
6. **Por que ACK após ATOMIC_MOVE?**  
   Garante durabilidade real no disco da réplica antes da confirmação ao líder.
7. **Por que usar o ID autenticado da sessão WebSocket?**  
   Impede que nós forjem o ID de terceiros na mensagem de texto do ACK.
