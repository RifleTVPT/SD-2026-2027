# Sprint E1-UDP01 — Comunicação baseada em Datagramas

Este diretório corresponde à primeira tarefa do sprint, identificada como `E1-UDP01`.

## Confirmação da organização

A estrutura do repositório indica que cada sprint fica em `Sprints/` e cada tarefa tem a sua pasta correspondente. Neste caso, a pasta correta é:

- `Sprints/E1-UDP01/`

O ficheiro `e1-udp01.url` aponta para a tarefa pública no GitHub, e o ficheiro `ilustração.html` contém a visualização da tarefa relacionada.

## Conteúdo deste sprint

- `e1-udp01.url` — ligação para a tarefa no GitHub
- `ilustração.html` — material visual da tarefa
- `ficha1-apoio/` — resumo teórico e respostas às questões da parte A

## Objetivo da tarefa

Implementar, ao nível da aplicação, a garantia de ordenação que o UDP não oferece. A solução deve basear-se em:

- `DatagramSocket` e `DatagramPacket`
- mensagens numeradas no formato `N,Mensagem`
- estado do servidor `L` = última mensagem aceite em ordem
- resposta `waitingfor,<L+1>` quando a ordem falha
- manutenção do servidor em funcionamento mesmo com mensagens fora de ordem ou mal formadas

## Fluxo de trabalho recomendado

Antes de iniciar a implementação, segue a ordem do guia do estudante:

1. Atualizar a branch principal
2. Criar uma branch para a tarefa (`tarefa-1`)
3. Resolver a tarefa
4. Fazer commit e push para o fork
5. Abrir Pull Request para avaliação

## Documentação de apoio

Para o estudo e compreensão da parte conceptual, consulta:

- `ficha1-apoio/materia-teorica-e-respostas.md`

## Regras da disciplina

- uma branch por tarefa
- nunca trabalhar diretamente na `master`
- atualizar a `master` antes de criar uma nova tarefa
- commits descritivos
- abrir PR para avaliação
- não submeter ficheiros gerados pela compilação nem pastas do IDE
