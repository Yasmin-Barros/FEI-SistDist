"""Bot cliente: faz login, lista canais e cria canais novos.

Não depende de interação humana. Se o serviço ainda não estiver no ar (ou
cair no meio do caminho), reenvia a requisição até obter resposta.
"""
import os
import random
import re
import time

import zmq

from common import ERRO, SUCESSO, info, log, pack, unpack

NAME = os.getenv("BOT_NAME", "bot-1")
BROKER = os.getenv("BROKER_ADDR", "tcp://broker:5555")
TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "3000"))
INTERVAL = float(os.getenv("INTERVAL_S", "4"))
ROUNDS = int(os.getenv("ROUNDS", "0"))  # 0 = roda para sempre
MIN_CHANNELS = int(os.getenv("MIN_CHANNELS", "4"))

CHANNEL_POOL = ["geral", "random", "dev", "noticias", "jogos", "musica", "filmes", "ciencia"]

ctx = zmq.Context()
sock = None
user = NAME  # nome efetivo (pode mudar se o login falhar)


def connect() -> None:
    global sock
    if sock is not None:
        sock.close(linger=0)
    sock = ctx.socket(zmq.REQ)
    sock.connect(BROKER)


def request(service: str, data: dict) -> dict:
    """Envia REQ e espera REP. Sem resposta no prazo: recria o socket e reenvia."""
    payload = pack(service, data)
    msg = unpack(payload)
    while True:
        sock.send(payload)
        log(user, "ENVIADA", msg, "broker")
        if sock.poll(TIMEOUT_MS):
            reply = unpack(sock.recv())
            log(user, "RECEBIDA", reply, "broker")
            return reply
        info(user, f"sem resposta em {TIMEOUT_MS} ms (serviço indisponível?), tentando de novo...")
        connect()


def do_login() -> None:
    """Repete o login até ser bem sucedido; em erro, corrige o nome e tenta de novo."""
    global user
    attempt = 0
    while True:
        reply = request("login", {"user": user})
        if reply["data"]["status"] == SUCESSO:
            info(user, f"login OK no {reply['data'].get('server')}")
            return
        attempt += 1
        info(user, f"login recusado: {reply['data'].get('description')}")
        sanitized = re.sub(r"[^A-Za-z0-9_.-]", "-", user)[:32] or f"bot-{random.randint(1000, 9999)}"
        user = sanitized if sanitized != user else f"{sanitized[:26]}-{random.randint(100, 999)}"
        info(user, f"tentando login novamente como '{user}' (tentativa {attempt + 1})")
        time.sleep(1)


def list_channels() -> list[str]:
    reply = request("channels", {"user": user})
    if reply["data"]["status"] != SUCESSO:
        info(user, f"erro ao listar canais: {reply['data'].get('description')}")
        return []
    return reply["data"]["channels"]


def create_channel(name: str) -> bool:
    reply = request("channel", {"user": user, "channel": name})
    if reply["data"]["status"] == SUCESSO:
        info(user, f"canal '{name}' criado")
        return True
    info(user, f"não foi possível criar '{name}': {reply['data'].get('description')}")
    return False


def main() -> None:
    connect()
    info(user, f"iniciando, broker em {BROKER}")
    do_login()

    n = 0
    while ROUNDS == 0 or n < ROUNDS:
        n += 1
        channels = list_channels()
        info(user, f"canais conhecidos: {channels}")
        # Critério: cria um canal se há poucos canais ou, com 25% de chance, para variar.
        if len(channels) < MIN_CHANNELS or random.random() < 0.25:
            candidates = [c for c in CHANNEL_POOL if c not in channels]
            if candidates:
                create_channel(random.choice(candidates))
        time.sleep(INTERVAL + random.random())


if __name__ == "__main__":
    main()
