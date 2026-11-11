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

