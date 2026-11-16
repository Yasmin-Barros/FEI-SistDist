# FEI – Sistemas Distribuídos

## Sistema de troca de mensagens

Projeto desenvolvido para a disciplina de Sistemas Distribuídos da FEI.

O projeto consiste em um sistema de troca de mensagens utilizando vários servidores, permitindo que clientes façam login, criem canais e enviem mensagens.

O sistema foi desenvolvido utilizando Python, Java, ZeroMQ, MessagePack e SQLite.

## Tecnologias utilizadas

- **Python:** implementação dos servidores, broker, proxy e serviço de referência.
- **Java:** implementação dos bots clientes.
- **ZeroMQ:** comunicação entre clientes, servidores e serviços.
- **MessagePack:** serialização das mensagens trocadas entre os processos.
- **SQLite:** armazenamento dos usuários, canais e mensagens.
- **Docker:** execução dos serviços em containers.

## Funcionamento do projeto

O sistema possui dois servidores Python, dois clientes Java, um broker, um proxy e um serviço de referência.

Os clientes enviam solicitações ao broker, que distribui as requisições entre os servidores disponíveis.

Os servidores processam as operações, armazenam os dados no SQLite e utilizam o proxy para distribuir as mensagens aos clientes inscritos nos canais.

Cada servidor possui seu próprio banco de dados.

## Componentes

- **Broker:** recebe as requisições dos clientes e distribui entre os servidores.
- **Proxy:** encaminha as mensagens publicadas para os clientes inscritos.
- **Reference:** atribui ranks aos servidores e acompanha sua disponibilidade por heartbeat.
- **Server 1 e Server 2:** processam as requisições e armazenam os dados.
- **Client 1 e Client 2:** bots Java que realizam login, criam canais, enviam e recebem mensagens.

## Funcionalidades implementadas

### Parte 1 – Login e canais

- Login dos usuários.
- Validação dos nomes de usuário.
- Criação de canais.
- Listagem dos canais disponíveis.
- Persistência dos dados utilizando SQLite.

### Parte 2 – Publicação e inscrição

- Publicação de mensagens nos canais.
- Inscrição dos clientes em diferentes canais.
- Comunicação utilizando o padrão Publisher/Subscriber.
- Distribuição das mensagens por meio do proxy.

### Parte 3 – Relógios lógicos e serviço de referência

- Implementação dos relógios lógicos de Lamport.
- Atribuição de ranks aos servidores.
- Listagem dos servidores disponíveis.
- Monitoramento dos servidores por heartbeat.

### Parte 4 – Eleição e sincronização de relógios

- Eleição de um coordenador utilizando o algoritmo Bully.
- Comunicação entre os servidores.
- Divulgação do coordenador pelo tópico Pub/Sub `servers`.
- Sincronização dos relógios físicos utilizando o algoritmo Berkeley.

### Parte 5 – Replicação de mensagens

- Replicação das mensagens entre os servidores.
- Cada servidor mantém uma cópia do histórico de mensagens.
- Identificação única das mensagens por meio de `replication_id`.
- Prevenção de mensagens duplicadas.
- Recuperação de mensagens ausentes quando um servidor retorna.
- Sincronização periódica entre os servidores a cada 30 segundos.

A replicação utiliza comunicação direta entre os servidores. Quando uma mensagem é publicada, ela é enviada imediatamente para os outros servidores. Caso algum servidor esteja indisponível, a sincronização periódica permite recuperar as mensagens posteriormente.

## Banco de dados

Cada servidor possui seu próprio banco SQLite:

- `data/server-1/server-1.db`
- `data/server-2/server-2.db`

Os bancos possuem as seguintes tabelas:

- `logins`: armazena os registros de login.
- `canais`: armazena os canais criados.
- `mensagens`: armazena as mensagens publicadas, incluindo seus identificadores de replicação.

## Como executar

É necessário ter Docker e Docker Compose instalados.

Na pasta `FEI-SistDist-parte-5`, execute:

```bash
docker compose up --build
```

Esse comando constrói as imagens e inicia todos os serviços do projeto.

Os bots Java começam a executar automaticamente, realizando login, criando canais, enviando mensagens e recebendo publicações.

Para encerrar a execução, utilize:

```bash
docker compose down
```

## Estrutura do projeto

```text
FEI-SistDist-parte-5/
├── Dockerfile
├── docker-compose.yaml
├── README.md
├── requirements.txt
├── data/
│   ├── server-1/
│   └── server-2/
├── python/
│   ├── broker.py
│   ├── client.py
│   ├── common.py
│   ├── proxy.py
│   ├── reference.py
│   └── server.py
└── java/
    ├── Dockerfile
    ├── pom.xml
    └── src/
        └── main/java/br/fei/sistdist/
            └── ClientBot.java
```

## Objetivo

O objetivo do projeto é aplicar conceitos de Sistemas Distribuídos, utilizando comunicação entre processos, distribuição de requisições, publicação e inscrição de mensagens, persistência de dados, relógios lógicos, sincronização de relógios, eleição de coordenador e replicação de informações entre servidores.