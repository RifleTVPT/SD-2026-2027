# tpf-replica - Sprint E2-03

## Compilacao

A partir da raiz do repositorio:

```powershell
New-Item -ItemType Directory -Force Sprints/E2-TP1/tpf-replica/out
javac -encoding UTF-8 `
  -d Sprints/E2-TP1/tpf-replica/out `
  (Get-ChildItem Sprints/E2-TP1/tpf-replica/src/tpf/*.java).FullName
```

## Execucao

Mudar para a pasta da replica:

```powershell
cd Sprints/E2-TP1/tpf-replica
```

Primeiro registo:

```powershell
java -cp out tpf.ReplicaService A ws://localhost:8765 8388608 <TOKEN>
```

Ligacao seguinte:

```powershell
java -cp out tpf.ReplicaService A ws://localhost:8765 8388608
```

O primeiro argumento e o ID, o segundo e o URL do hub e o terceiro e a
quota em bytes. O token apenas e necessario no primeiro registo.

## Chaves e ficheiros

No arranque, a replica:

- gera `dados-<id>/chave.priv` em PKCS#8;
- gera `dados-<id>/chave.pub` em X.509;
- restringe os ficheiros ao utilizador no sistema operativo;
- soma o tamanho dos ficheiros `.bin` existentes.

O ficheiro de chave privada nao deve entrar no Git.

## Eventos

Os eventos sao escritos em `eventos-<id>.txt`:

- `LIGADO`;
- `DESLIGADO`;
- `RELIGAR <espera-ms>`;
- `STORE`;
- `QUOTA`;
- `ERRO`.

## STORE

Para cada `STORE`:

1. rejeita se `usados + tamanho > quota`;
2. calcula SHA-256 do `.bin`;
3. rejeita se o hash nao coincide com o chunkId;
4. grava `.bin` e `.hdr` por ficheiros temporarios e ATOMIC_MOVE;
5. atualiza usados;
6. publica o ACK pelo topico `tpf-lider`.
