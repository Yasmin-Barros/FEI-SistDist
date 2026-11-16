"""Servidor: atende login, criação e listagem de canais.

Cada servidor tem o seu próprio banco SQLite (em DATA_DIR/<SERVER_NAME>.db),
que NÃO é compartilhado com os outros servidores.
"""
import os
import sqlite3

import zmq

import common
from common import ERRO, SUCESSO, CHANNEL_RE, USER_RE, info, log, pack, unpack

NAME = os.getenv("SERVER_NAME", "server-1")
BROKER = os.getenv("BROKER_ADDR", "tcp://broker:5556")
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
            nome      TEXT PRIMARY KEY,
            criado_por TEXT,
            timestamp REAL NOT NULL           -- timestamp de envio da criação
        );
        """
    )
    db.commit()
    return db


def ok(**extra) -> dict:
    return {"status": SUCESSO, "server": NAME, **extra}


def err(description: str) -> dict:
    return {"status": ERRO, "server": NAME, "description": description}


def handle_login(db, msg) -> dict:
    user = msg["data"].get("user")
    if not isinstance(user, str) or not USER_RE.match(user):
        return err("nome de usuário inválido (use 1-32 caracteres: letras, números, '_', '-' ou '.')")
    db.execute("INSERT INTO logins (usuario, timestamp) VALUES (?, ?)", (user, msg["timestamp"]))
    db.commit()
    return ok(user=user)


def handle_channel(db, msg) -> dict:
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


def handle_channels(db, msg) -> dict:
    rows = db.execute("SELECT nome FROM canais ORDER BY nome").fetchall()
    return ok(channels=[r[0] for r in rows])


HANDLERS = {"login": handle_login, "channel": handle_channel, "channels": handle_channels}


def process(db, raw: bytes) -> tuple[str, dict]:
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
        return service, handler(db, msg)
    except Exception as e:  # erro interno não pode derrubar o servidor
        return service, err(f"erro interno: {e}")


def main() -> None:
    db = open_db()
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER)
    n_login = db.execute("SELECT COUNT(*) FROM logins").fetchone()[0]
    n_canais = db.execute("SELECT COUNT(*) FROM canais").fetchone()[0]
    info(NAME, f"ativo, conectado a {BROKER} | dados anteriores: {n_login} logins, {n_canais} canais")

    while True:
        raw = sock.recv()
        service, reply_data = process(db, raw)
        reply = pack(service, reply_data)
        sock.send(reply)
        log(NAME, "ENVIADA", unpack(reply), "broker")


if __name__ == "__main__":
    main()
