"""Servidor com relógio lógico, eleição Bully e sincronização Berkeley.

Cada servidor mantém seu banco local. A comunicação entre servidores usa
MessagePack sobre ZeroMQ; a eleição do coordenador é anunciada no tópico
Pub/Sub `servers`.
"""
import os
import sqlite3
import threading
import time
import uuid

import zmq

from common import (
    CHANNEL_RE, ERRO, SUCESSO, USER_RE, info, log, pack, unpack, LamportClock,
)

NAME = os.getenv("SERVER_NAME", "server-1")
BROKER = os.getenv("BROKER_ADDR", "tcp://broker:5556")
PROXY_PUB = os.getenv("PROXY_PUB_ADDR", "tcp://proxy:5557")
PROXY_SUB = os.getenv("PROXY_SUB_ADDR", "tcp://proxy:5558")
DATA_DIR = os.getenv("DATA_DIR", "/data")
REFERENCE = os.getenv("REFERENCE_ADDR", "tcp://reference:5559")

# Portas internas fixas para os dois servidores previstos no compose da Parte 3.
# Podem ser sobrescritas por PEER_PORT e PEER_ADDRESSES sem alterar o protocolo.
_DEFAULT_PEERS = {
    "server-1": "tcp://server-1:5560",
    "server-2": "tcp://server-2:5561",
}
PEER_PORT = int(os.getenv("PEER_PORT", "5560" if NAME == "server-1" else "5561"))
PEER_ADDRESSES = dict(_DEFAULT_PEERS)
for item in os.getenv("PEER_ADDRESSES", "").split(","):
    if "=" in item:
        peer_name, endpoint = item.split("=", 1)
        PEER_ADDRESSES[peer_name.strip()] = endpoint.strip()

CLOCK = LamportClock()
CLOCK_LOCK = threading.Lock()
STATE_LOCK = threading.RLock()
PHYSICAL_OFFSET = 0.0
CLIENT_MESSAGES = 0
SERVER_RANK = None
COORDINATOR_NAME = None
ELECTION_REQUESTED = threading.Event()


def current_time() -> float:
    with STATE_LOCK:
        return time.time() + PHYSICAL_OFFSET


def server_pack(service: str, data: dict) -> bytes:
    with CLOCK_LOCK:
        return pack(service, data, CLOCK, current_time())


def receive_logical(msg: dict) -> None:
    with CLOCK_LOCK:
        CLOCK.received(msg.get("logical_clock"))


def reference_request(sock: zmq.Socket, service: str, data: dict) -> dict:
    payload = server_pack(service, data)
    sent = unpack(payload)
    sock.send(payload)
    log(NAME, "ENVIADA", sent, "reference")
    reply = unpack(sock.recv())
    receive_logical(reply)
    log(NAME, "RECEBIDA", reply, "reference")
    return reply


def peer_request(peer_name: str, service: str, data: dict, timeout_ms: int = 1500) -> dict:
    endpoint = PEER_ADDRESSES.get(peer_name)
    if not endpoint:
        raise RuntimeError(f"endereço do servidor {peer_name!r} desconhecido")
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.connect(endpoint)
    try:
        payload = server_pack("peer", {"action": service, "from": NAME,
                                        "rank": SERVER_RANK, **data})
        sock.send(payload)
        reply = unpack(sock.recv())
        receive_logical(reply)
        return reply.get("data", {})
    finally:
        sock.close(0)


def available_servers(ref_sock: zmq.Socket) -> list:
    try:
        reply = reference_request(ref_sock, "list", {})
        data = reply.get("data", {})
        if data.get("status") == SUCESSO:
            return data.get("servers", [])
    except Exception as exc:
        info(NAME, f"não foi possível consultar servidores disponíveis: {exc}")
    return []


def publish_coordinator(ctx: zmq.Context, coordinator: str) -> None:
    """Publica o coordenador no tópico obrigatório `servers`."""
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.LINGER, 0)
    pub.connect(PROXY_PUB)
    time.sleep(0.25)  # tempo para o proxy estabelecer a assinatura/publicação
    envelope = server_pack("pub", {"topic": "servers", "coordinator": coordinator,
                                   "server": NAME})
    pub.send_multipart([b"servers", envelope])
    log(NAME, "ENVIADA", unpack(envelope), "pub:servers")
    pub.close(0)


def become_coordinator(ctx: zmq.Context) -> None:
    global COORDINATOR_NAME
    with STATE_LOCK:
        COORDINATOR_NAME = NAME
    info(NAME, f"ELEITO COORDENADOR | rank={SERVER_RANK}")
    publish_coordinator(ctx, NAME)


def run_election(ref_sock: zmq.Socket, ctx: zmq.Context) -> None:
    """Eleição Bully: consulta servidores de rank maior; sem resposta, vence."""
    global COORDINATOR_NAME
    ELECTION_REQUESTED.clear()
    servers = available_servers(ref_sock)
    higher = [s for s in servers
              if s.get("name") != NAME and isinstance(s.get("rank"), int)
              and s["rank"] > SERVER_RANK]
    answered = []
    for server in sorted(higher, key=lambda s: s["rank"]):
        try:
            result = peer_request(server["name"], "election", {})
            if result.get("status") == SUCESSO:
                answered.append(server["name"])
        except Exception as exc:
            info(NAME, f"servidor superior {server['name']} não respondeu à eleição: {exc}")
    if answered:
        info(NAME, f"eleição encaminhada para servidor(es) de maior rank: {answered}")
        return
    become_coordinator(ctx)


def berkeley_time() -> float:
    """Coordenador calcula a média dos relógios disponíveis (algoritmo Berkeley)."""
    global PHYSICAL_OFFSET
    times = [current_time()]
    # Consulta os outros servidores por seus horários locais.
    ctx = zmq.Context.instance()
    ref = ctx.socket(zmq.REQ)
    ref.setsockopt(zmq.LINGER, 0)
    ref.setsockopt(zmq.SNDTIMEO, 1000)
    ref.setsockopt(zmq.RCVTIMEO, 1000)
    ref.connect(REFERENCE)
    try:
        servers = available_servers(ref)
    finally:
        ref.close(0)
    for server in servers:
        peer_name = server.get("name")
        if peer_name == NAME or peer_name not in PEER_ADDRESSES:
            continue
        try:
            result = peer_request(peer_name, "clock_probe", {})
            remote_time = result.get("physical_time")
            if isinstance(remote_time, (int, float)):
                times.append(float(remote_time))
        except Exception as exc:
            info(NAME, f"Berkeley: sem resposta de {peer_name}: {exc}")
    average = sum(times) / len(times)
    with STATE_LOCK:
        PHYSICAL_OFFSET = average - time.time()
    info(NAME, f"Berkeley concluído | amostras={len(times)} | hora média={average:.6f} | offset={PHYSICAL_OFFSET:+.6f}s")
    return average


def synchronize_with_coordinator() -> None:
    global PHYSICAL_OFFSET
    with STATE_LOCK:
        coordinator = COORDINATOR_NAME
    if not coordinator:
        raise RuntimeError("coordenador ainda não conhecido")
    if coordinator == NAME:
        berkeley_time()
        return
    result = peer_request(coordinator, "clock", {})
    corrected = result.get("physical_time")
    if result.get("status") != SUCESSO or not isinstance(corrected, (int, float)):
        raise RuntimeError(f"resposta inválida do coordenador {coordinator}: {result}")
    with STATE_LOCK:
        PHYSICAL_OFFSET = float(corrected) - time.time()
    info(NAME, f"relógio sincronizado com {coordinator} | offset={PHYSICAL_OFFSET:+.6f}s")


def insert_replica_message(db: sqlite3.Connection, item: dict) -> bool:
    """Persiste mensagem recebida de outra réplica sem criar duplicatas."""
    required = ("replication_id", "channel", "user", "message", "timestamp_envio", "timestamp_pub")
    if not isinstance(item, dict) or any(key not in item for key in required):
        raise ValueError("registro de réplica incompleto")
    db.execute("INSERT OR IGNORE INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)",
               (item["channel"], item["user"], item["timestamp_envio"]))
    cursor = db.execute("""INSERT OR IGNORE INTO mensagens
        (replication_id, canal, usuario, mensagem, timestamp_envio, timestamp_pub)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (item["replication_id"], item["channel"], item["user"], item["message"],
         item["timestamp_envio"], item["timestamp_pub"]))
    db.commit()
    return cursor.rowcount > 0


def export_messages(db: sqlite3.Connection) -> list:
    rows = db.execute("""SELECT replication_id, canal, usuario, mensagem, timestamp_envio, timestamp_pub
                         FROM mensagens ORDER BY timestamp_pub, replication_id""").fetchall()
    return [{"replication_id": r[0], "channel": r[1], "user": r[2], "message": r[3],
             "timestamp_envio": r[4], "timestamp_pub": r[5]} for r in rows]


def replicate_to_peers(item: dict) -> None:
    """Propaga a nova mensagem para os demais servidores configurados."""
    for peer_name in PEER_ADDRESSES:
        if peer_name == NAME:
            continue
        try:
            result = peer_request(peer_name, "replicate_message", {"message": item})
            if result.get("status") == SUCESSO:
                info(NAME, f"mensagem replicada para {peer_name}")
            else:
                info(NAME, f"réplica recusada por {peer_name}: {result}")
        except Exception as exc:
            info(NAME, f"réplica imediata para {peer_name} falhou; será recuperada depois: {exc}")


def synchronize_replica(db: sqlite3.Connection) -> None:
    """Anti-entropia: busca o histórico dos pares e insere somente itens ausentes."""
    for peer_name in PEER_ADDRESSES:
        if peer_name == NAME:
            continue
        try:
            result = peer_request(peer_name, "get_messages", {}, timeout_ms=5000)
            if result.get("status") != SUCESSO:
                info(NAME, f"não foi possível obter histórico de {peer_name}: {result}")
                continue
            inserted = sum(1 for item in result.get("messages", []) if insert_replica_message(db, item))
            info(NAME, f"anti-entropia com {peer_name}: {inserted} mensagem(ns) recuperada(s)")
        except Exception as exc:
            info(NAME, f"anti-entropia com {peer_name} indisponível: {exc}")


def peer_service_loop() -> None:
    """Atende eleição/relógio e escuta anúncios no tópico `servers`.

    Todos os sockets desta rotina são criados e usados somente nesta thread.
    """
    global COORDINATOR_NAME
    ctx = zmq.Context.instance()
    rep = ctx.socket(zmq.REP)
    rep.setsockopt(zmq.LINGER, 0)
    rep.bind(f"tcp://*:{PEER_PORT}")
    # Conexão SQLite exclusiva desta thread.
    peer_db = open_db()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 0)
    sub.setsockopt(zmq.SUBSCRIBE, b"servers")
    sub.connect(PROXY_SUB)
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)
    info(NAME, f"serviço entre servidores ativo na porta {PEER_PORT}")

    while True:
        events = dict(poller.poll(500))
        if sub in events:
            try:
                frames = sub.recv_multipart()
                if len(frames) >= 2 and frames[0] == b"servers":
                    announcement = unpack(frames[1])
                    receive_logical(announcement)
                    coordinator = announcement.get("data", {}).get("coordinator")
                    if isinstance(coordinator, str) and coordinator:
                        with STATE_LOCK:
                            COORDINATOR_NAME = coordinator
                        info(NAME, f"coordenador atualizado via Pub/Sub: {coordinator}")
            except Exception as exc:
                info(NAME, f"erro ao receber anúncio de coordenador: {exc}")

        if rep in events:
            try:
                msg = unpack(rep.recv())
                receive_logical(msg)
                data = msg.get("data") or {}
                action = data.get("action")
                if action == "election":
                    sender_rank = data.get("rank")
                    if isinstance(sender_rank, int) and SERVER_RANK > sender_rank:
                        ELECTION_REQUESTED.set()
                    result = {"status": SUCESSO, "server": NAME, "rank": SERVER_RANK}
                elif action == "clock_probe":
                    result = {"status": SUCESSO, "server": NAME,
                              "physical_time": current_time()}
                elif action == "clock":
                    with STATE_LOCK:
                        is_coordinator = COORDINATOR_NAME == NAME
                    if is_coordinator:
                        corrected = berkeley_time()
                        result = {"status": SUCESSO, "coordinator": NAME,
                                  "physical_time": corrected}
                    else:
                        result = {"status": ERRO, "coordinator": COORDINATOR_NAME,
                                  "description": "este servidor não é o coordenador"}
                elif action == "replicate_message":
                    inserted = insert_replica_message(peer_db, data.get("message"))
                    result = {"status": SUCESSO, "inserted": inserted, "server": NAME}
                elif action == "get_messages":
                    result = {"status": SUCESSO, "server": NAME, "messages": export_messages(peer_db)}
                else:
                    result = {"status": ERRO, "description": f"ação desconhecida: {action}"}
                reply = server_pack("peer_reply", result)
                rep.send(reply)
                log(NAME, "ENVIADA", unpack(reply), f"peer:{data.get('from', '?')}")
            except Exception as exc:
                # REP precisa responder mesmo quando a requisição é inválida.
                try:
                    rep.send(server_pack("peer_reply", {"status": ERRO,
                                                         "description": str(exc)}))
                except Exception:
                    pass
                info(NAME, f"erro no serviço entre servidores: {exc}")


def open_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    db = sqlite3.connect(os.path.join(DATA_DIR, f"{NAME}.db"))
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS logins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT NOT NULL,
            timestamp REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS canais (
            nome TEXT PRIMARY KEY,
            criado_por TEXT,
            timestamp REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS mensagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            replication_id TEXT,
            canal TEXT NOT NULL,
            usuario TEXT NOT NULL,
            mensagem TEXT NOT NULL,
            timestamp_envio REAL NOT NULL,
            timestamp_pub REAL NOT NULL
        );
        """
    )
    # Migração compatível com bancos criados nas Partes anteriores.
    columns = {row[1] for row in db.execute("PRAGMA table_info(mensagens)")}
    if "replication_id" not in columns:
        db.execute("ALTER TABLE mensagens ADD COLUMN replication_id TEXT")
    legacy_rows = db.execute("SELECT id FROM mensagens WHERE replication_id IS NULL").fetchall()
    for (row_id,) in legacy_rows:
        db.execute("UPDATE mensagens SET replication_id = ? WHERE id = ?", (uuid.uuid4().hex, row_id))
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mensagens_replication_id ON mensagens(replication_id)")
    db.commit()
    return db


def ok(**extra) -> dict:
    return {"status": SUCESSO, "server": NAME, **extra}


def err(description: str) -> dict:
    return {"status": ERRO, "server": NAME, "description": description}


def handle_login(db: sqlite3.Connection, pub_sock: zmq.Socket, msg: dict) -> dict:
    user = msg["data"].get("user")
    if not isinstance(user, str) or not USER_RE.match(user):
        return err("nome de usuário inválido (use 1-32 caracteres: letras, números, '_', '-' ou '.')")
    db.execute("INSERT INTO logins (usuario, timestamp) VALUES (?, ?)", (user, msg["timestamp"]))
    db.commit()
    return ok(user=user)


def handle_channel(db: sqlite3.Connection, pub_sock: zmq.Socket, msg: dict) -> dict:
    name = msg["data"].get("channel")
    user = msg["data"].get("user")
    if not isinstance(name, str) or not CHANNEL_RE.match(name):
        return err("nome de canal inválido")
    try:
        db.execute("INSERT INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)",
                   (name, user if isinstance(user, str) else None, msg["timestamp"]))
        db.commit()
    except sqlite3.IntegrityError:
        return err(f"o canal '{name}' já existe")
    return ok(channel=name)


def handle_channels(db: sqlite3.Connection, pub_sock: zmq.Socket, msg: dict) -> dict:
    rows = db.execute("SELECT nome FROM canais ORDER BY nome").fetchall()
    return ok(channels=[r[0] for r in rows])


def handle_publish(db: sqlite3.Connection, pub_sock: zmq.Socket, msg: dict) -> dict:
    data = msg["data"]
    channel, user, message = data.get("channel"), data.get("user"), data.get("message")
    if not isinstance(channel, str) or not CHANNEL_RE.match(channel):
        return err("nome de canal inválido")
    if not isinstance(user, str) or not USER_RE.match(user):
        return err("nome de usuário inválido")
    if not isinstance(message, str) or not message.strip():
        return err("a mensagem não pode ser vazia")
    db.execute("INSERT OR IGNORE INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)",
               (channel, user, msg["timestamp"]))
    pub_data = {"channel": channel, "user": user, "message": message,
                "server": NAME, "client_timestamp": msg["timestamp"],
                "client_logical_clock": msg.get("logical_clock")}
    pub_envelope = server_pack("pub", pub_data)
    pub_ts = unpack(pub_envelope)["timestamp"]
    replication_id = uuid.uuid4().hex
    db.execute("""INSERT INTO mensagens
        (replication_id, canal, usuario, mensagem, timestamp_envio, timestamp_pub)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (replication_id, channel, user, message, msg["timestamp"], pub_ts))
    db.commit()
    replicate_to_peers({
        "replication_id": replication_id, "channel": channel, "user": user,
        "message": message, "timestamp_envio": msg["timestamp"], "timestamp_pub": pub_ts,
    })
    # Somente o servidor que recebeu a requisição publica para os clientes.
    pub_sock.send_multipart([channel.encode("utf-8"), pub_envelope])
    log(NAME, "ENVIADA", unpack(pub_envelope), f"pub:{channel}")
    return ok(channel=channel, published_at=pub_ts)


HANDLERS = {"login": handle_login, "channel": handle_channel,
            "channels": handle_channels, "publish": handle_publish}


def process(db: sqlite3.Connection, pub_sock: zmq.Socket, raw: bytes) -> tuple[str, dict]:
    try:
        msg = unpack(raw)
        log(NAME, "RECEBIDA", msg, "broker")
    except Exception:
        return "erro", err("mensagem inválida (não é MessagePack)")
    if not isinstance(msg, dict) or not isinstance(msg.get("data"), dict):
        return "erro", err("envelope inválido (esperado: service, timestamp, data)")
    if not isinstance(msg.get("timestamp"), (int, float)):
        return str(msg.get("service")), err("mensagem sem timestamp de envio")
    if not isinstance(msg.get("logical_clock"), int):
        return str(msg.get("service")), err("mensagem sem relógio lógico")
    receive_logical(msg)
    service = msg.get("service")
    handler = HANDLERS.get(service)
    if handler is None:
        return str(service), err(f"serviço desconhecido: {service!r}")
    try:
        return service, handler(db, pub_sock, msg)
    except Exception as exc:
        return service, err(f"erro interno: {exc}")


def main() -> None:
    global CLIENT_MESSAGES, SERVER_RANK
    db = open_db()
    ctx = zmq.Context.instance()
    rep_sock = ctx.socket(zmq.REP)
    rep_sock.setsockopt(zmq.RCVTIMEO, 500)
    rep_sock.connect(BROKER)
    ref_sock = ctx.socket(zmq.REQ)
    ref_sock.connect(REFERENCE)
    ref_sock.setsockopt(zmq.RCVTIMEO, 2000)
    ref_sock.setsockopt(zmq.SNDTIMEO, 2000)

    while SERVER_RANK is None:
        try:
            rank_reply = reference_request(ref_sock, "rank", {"name": NAME})
            if rank_reply.get("data", {}).get("status") == SUCESSO:
                SERVER_RANK = rank_reply["data"].get("rank")
            else:
                info(NAME, f"falha ao obter rank: {rank_reply.get('data')}")
        except Exception as exc:
            info(NAME, f"referência indisponível ({exc}); nova tentativa em 2s")
            time.sleep(2)

    # Serviço ponto-a-ponto e assinatura do anúncio do coordenador.
    threading.Thread(target=peer_service_loop, name=f"peer-{NAME}", daemon=True).start()
    time.sleep(0.5)  # aguarda o bind da porta peer
    synchronize_replica(db)  # recupera mensagens acumuladas enquanto esteve desligado
    last_replica_sync = time.monotonic()
    pub_sock = ctx.socket(zmq.PUB)
    pub_sock.connect(PROXY_PUB)
    time.sleep(0.5)

    n_login = db.execute("SELECT COUNT(*) FROM logins").fetchone()[0]
    n_canais = db.execute("SELECT COUNT(*) FROM canais").fetchone()[0]
    n_msgs = db.execute("SELECT COUNT(*) FROM mensagens").fetchone()[0]
    info(NAME, f"ativo | rank={SERVER_RANK} | broker={BROKER} | reference={REFERENCE} | "
                f"peer_port={PEER_PORT} | dados anteriores: {n_login} logins, {n_canais} canais, {n_msgs} mensagens")

    # Pequeno atraso para que o serviço peer esteja escutando antes da eleição inicial.
    time.sleep(0.5)
    run_election(ref_sock, ctx)

    while True:
        try:
            raw = rep_sock.recv()
        except zmq.Again:
            # Permite processar eleição e anti-entropia sem tráfego de clientes.
            if ELECTION_REQUESTED.is_set():
                run_election(ref_sock, ctx)
            if time.monotonic() - last_replica_sync >= 30:
                synchronize_replica(db)
                last_replica_sync = time.monotonic()
            continue
        CLIENT_MESSAGES += 1
        service, reply_data = process(db, pub_sock, raw)
        reply = server_pack(service, reply_data)
        rep_sock.send(reply)
        log(NAME, "ENVIADA", unpack(reply), "broker")

        if ELECTION_REQUESTED.is_set():
            run_election(ref_sock, ctx)

        # Heartbeat permanece a cada 10 requisições, mas não sincroniza mais a hora.
        if CLIENT_MESSAGES % 10 == 0:
            try:
                hb = reference_request(ref_sock, "heartbeat", {"name": NAME, "rank": SERVER_RANK})
                hb_data = hb.get("data", {})
                if hb_data.get("status") == SUCESSO:
                    info(NAME, f"heartbeat OK | rank={SERVER_RANK} | servidores={hb_data.get('servers', [])}")
                else:
                    info(NAME, f"heartbeat recusado: {hb_data}")
            except Exception as exc:
                info(NAME, f"falha no heartbeat: {exc}")

        # Berkeley: a cada 15 mensagens de clientes, atualiza o relógio físico.
        if CLIENT_MESSAGES % 15 == 0:
            try:
                synchronize_with_coordinator()
            except Exception as exc:
                info(NAME, f"coordenador indisponível ({exc}); iniciando eleição")
                run_election(ref_sock, ctx)
                # Se acabou de ser eleito, executa o ajuste Berkeley local.
                try:
                    synchronize_with_coordinator()
                except Exception as sync_exc:
                    info(NAME, f"não foi possível sincronizar após eleição: {sync_exc}")


if __name__ == "__main__":
    main()
