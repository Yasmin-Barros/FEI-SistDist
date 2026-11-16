"""Servidor: atende login, criação/listagem de canais e publicação via Pub/Sub.

Cada servidor tem o seu próprio banco SQLite (em DATA_DIR/<SERVER_NAME>.db),
que NÃO é compartilhado com os outros servidores.
"""
import os
import sqlite3
import time

import zmq

from common import (
    CHANNEL_RE,
    ERRO,
    SUCESSO,
    USER_RE,
    info,
    log,
    pack,
    pack_pub,
    unpack,
)

NAME = os.getenv("SERVER_NAME", "server-1")
BROKER = os.getenv("BROKER_ADDR", "tcp://broker:5556")
PROXY_PUB = os.getenv("PROXY_PUB_ADDR", "tcp://proxy:5557")
DATA_DIR = os.getenv("DATA_DIR", "/data")


def open_db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    db = sqlite3.connect(os.path.join(DATA_DIR, f"{NAME}.db"))
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS logins (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario   TEXT NOT NULL,
            timestamp REAL NOT NULL           -- timestamp de envio do login
        );
        CREATE TABLE IF NOT EXISTS canais (
            nome       TEXT PRIMARY KEY,
            criado_por TEXT,
            timestamp  REAL NOT NULL          -- timestamp de envio da criação
        );
        CREATE TABLE IF NOT EXISTS mensagens (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            canal           TEXT NOT NULL,
            usuario         TEXT NOT NULL,
            mensagem        TEXT NOT NULL,
            timestamp_envio REAL NOT NULL,    -- timestamp de envio do cliente
            timestamp_pub   REAL NOT NULL     -- timestamp de publicação do servidor
        );
        """
    )
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
        return err("nome de canal inválido (use 1-32 caracteres minúsculos: a-z, 0-9, '_' ou '-')")
    try:
        db.execute(
            "INSERT INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)",
            (name, user if isinstance(user, str) else None, msg["timestamp"]),
        )
        db.commit()
    except sqlite3.IntegrityError:
        return err(f"o canal '{name}' já existe")
    return ok(channel=name)


def handle_channels(db: sqlite3.Connection, pub_sock: zmq.Socket, msg: dict) -> dict:
    rows = db.execute("SELECT nome FROM canais ORDER BY nome").fetchall()
    return ok(channels=[r[0] for r in rows])


def handle_publish(db: sqlite3.Connection, pub_sock: zmq.Socket, msg: dict) -> dict:
    data = msg["data"]
    channel = data.get("channel")
    user = data.get("user")
    message = data.get("message")

    if not isinstance(channel, str) or not CHANNEL_RE.match(channel):
        return err("nome de canal inválido")
    if not isinstance(user, str) or not USER_RE.match(user):
        return err("nome de usuário inválido")
    if not isinstance(message, str) or not message.strip():
        return err("a mensagem não pode ser vazia")

    db.execute(
        "INSERT OR IGNORE INTO canais (nome, criado_por, timestamp) VALUES (?, ?, ?)",
        (channel, user, msg["timestamp"]),
    )

    pub_frames = pack_pub(
        channel,
        {
            "user": user,
            "message": message,
            "server": NAME,
            "client_timestamp": msg["timestamp"],
        },
    )
    pub_envelope = unpack(pub_frames[1])
    pub_ts = pub_envelope["timestamp"]

    db.execute(
        """
        INSERT INTO mensagens (canal, usuario, mensagem, timestamp_envio, timestamp_pub)
        VALUES (?, ?, ?, ?, ?)
        """,
        (channel, user, message, msg["timestamp"], pub_ts),
    )
    db.commit()

    pub_sock.send_multipart(pub_frames)
    log(NAME, "ENVIADA", pub_envelope, f"pub:{channel}")
    return ok(channel=channel, published_at=pub_ts)


HANDLERS = {
    "login": handle_login,
    "channel": handle_channel,
    "channels": handle_channels,
    "publish": handle_publish,
}


def process(db: sqlite3.Connection, pub_sock: zmq.Socket, raw: bytes) -> tuple[str, dict]:
    """Devolve (service, resposta). Nunca levanta exceção: REP sempre precisa responder."""
    try:
        msg = unpack(raw)
        log(NAME, "RECEBIDA", msg, "broker")
    except Exception:
        return "erro", err("mensagem inválida (não é MessagePack)")
    if not isinstance(msg, dict) or not isinstance(msg.get("data"), dict):
        return "erro", err("envelope inválido (esperado: service, timestamp, data)")
    if not isinstance(msg.get("timestamp"), (int, float)):
        return str(msg.get("service")), err("mensagem sem timestamp de envio")
    service = msg.get("service")
    handler = HANDLERS.get(service)
    if handler is None:
        return str(service), err(f"serviço desconhecido: {service!r}")
    try:
        return service, handler(db, pub_sock, msg)
    except Exception as e:
        return service, err(f"erro interno: {e}")


def main() -> None:
    db = open_db()
    ctx = zmq.Context()

    rep_sock = ctx.socket(zmq.REP)
    rep_sock.connect(BROKER)

    pub_sock = ctx.socket(zmq.PUB)
    pub_sock.connect(PROXY_PUB)
    time.sleep(0.2)  # evita slow-joiner inicial no socket PUB

    n_login = db.execute("SELECT COUNT(*) FROM logins").fetchone()[0]
    n_canais = db.execute("SELECT COUNT(*) FROM canais").fetchone()[0]
    n_msgs = db.execute("SELECT COUNT(*) FROM mensagens").fetchone()[0]
    info(
        NAME,
        f"ativo | broker={BROKER} | proxy_pub={PROXY_PUB} | "
        f"dados anteriores: {n_login} logins, {n_canais} canais, {n_msgs} mensagens",
    )

    while True:
        raw = rep_sock.recv()
        service, reply_data = process(db, pub_sock, raw)
        reply = pack(service, reply_data)
        rep_sock.send(reply)
        log(NAME, "ENVIADA", unpack(reply), "broker")


if __name__ == "__main__":
    main()
