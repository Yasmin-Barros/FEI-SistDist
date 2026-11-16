"""Bot cliente em Python (mantido como referência / compatibilidade)."""
import os
import random
import re
import threading
import time
from datetime import datetime

import zmq

from common import (
    SUCESSO,
    fmt_ts,
    info,
    log,
    pack,
    unpack,
    unpack_pub,
    LamportClock,
)

NAME = os.getenv("BOT_NAME", "bot-py-1")
BROKER = os.getenv("BROKER_ADDR", "tcp://broker:5555")
PROXY_SUB = os.getenv("PROXY_SUB_ADDR", "tcp://proxy:5558")
TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "3000"))

MIN_CHANNELS = 5
MIN_SUBSCRIPTIONS = 3
MESSAGES_PER_BURST = 10
PUBLISH_INTERVAL_S = 1.0

CHANNEL_POOL = [
    "geral", "random", "dev", "noticias", "jogos",
    "musica", "filmes", "ciencia", "esportes", "tecnologia",
]

SAMPLE_MESSAGES = [
    "Olá pessoal, testando o canal!",
    "Alguém online para discutir sistemas distribuídos?",
    "ZeroMQ + MessagePack funcionando perfeitamente.",
    "Atualização de status do bot em execução.",
    "Mensagem automática enviada para teste de Pub/Sub.",
    "Verificando latência entre publicação e recebimento.",
    "Mais uma rodada de testes no BBS distribuído!",
]

ctx = zmq.Context()
req_sock = None
sub_sock = None
sub_lock = threading.Lock()
subscribed_channels: set[str] = set()
user = NAME
clock = LamportClock()


def connect_req() -> None:
    global req_sock
    if req_sock is not None:
        req_sock.close(linger=0)
    req_sock = ctx.socket(zmq.REQ)
    req_sock.connect(BROKER)


def start_subscriber() -> None:
    global sub_sock
    sub_sock = ctx.socket(zmq.SUB)
    sub_sock.connect(PROXY_SUB)

    def _listen() -> None:
        while True:
            with sub_lock:
                has_event = sub_sock.poll(100)
                frames = sub_sock.recv_multipart() if has_event else None
            if frames:
                recv_ts = time.time()
                topic, msg = unpack_pub(frames)
                clock.received(msg.get("logical_clock"))
                data = msg.get("data", {})
                send_ts = msg.get("timestamp")
                recv_fmt = datetime.fromtimestamp(recv_ts).strftime("%H:%M:%S.%f")[:-3]
                print(
                    f"[{recv_fmt}] {user:<10} PUB-RECV  <- #{topic:<10} | "
                    f"autor={data.get('user')} | msg={data.get('message')!r} | "
                    f"ts_envio={fmt_ts(send_ts)} | ts_recebimento={fmt_ts(recv_ts)}",
                    flush=True,
                )

    threading.Thread(target=_listen, daemon=True).start()


def subscribe_channel(channel: str) -> None:
    with sub_lock:
        if channel in subscribed_channels:
            return
        sub_sock.setsockopt_string(zmq.SUBSCRIBE, channel)
        subscribed_channels.add(channel)
    info(user, f"inscrito no canal '{channel}' (total inscritos: {sorted(subscribed_channels)})")


def request(service: str, data: dict) -> dict:
    payload = pack(service, data, clock)
    msg = unpack(payload)
    while True:
        req_sock.send(payload)
        log(user, "ENVIADA", msg, "broker")
        if req_sock.poll(TIMEOUT_MS):
            reply = unpack(req_sock.recv())
            clock.received(reply.get("logical_clock"))
            log(user, "RECEBIDA", reply, "broker")
            return reply
        info(user, f"sem resposta em {TIMEOUT_MS} ms (serviço indisponível?), tentando de novo...")
        connect_req()


def do_login() -> None:
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
        return []
    return reply["data"]["channels"]


def create_channel(name: str) -> bool:
    reply = request("channel", {"user": user, "channel": name})
    if reply["data"]["status"] == SUCESSO:
        info(user, f"canal '{name}' criado")
        return True
    info(user, f"não foi possível criar '{name}': {reply['data'].get('description')}")
    return False


def publish_message(channel: str, text: str) -> bool:
    reply = request("publish", {"user": user, "channel": channel, "message": text})
    return reply["data"]["status"] == SUCESSO


def sync_channels_and_subscriptions() -> list[str]:
    channels = list_channels()
    if len(channels) < MIN_CHANNELS:
        candidates = [c for c in CHANNEL_POOL if c not in channels]
        if candidates:
            new_ch = random.choice(candidates)
            if create_channel(new_ch) and new_ch not in channels:
                channels.append(new_ch)
    if not channels and create_channel("geral"):
        channels = ["geral"]
    if len(subscribed_channels) < MIN_SUBSCRIPTIONS:
        unsubscribed = [c for c in channels if c not in subscribed_channels]
        if unsubscribed:
            subscribe_channel(random.choice(unsubscribed))
    return channels


def main() -> None:
    connect_req()
    start_subscriber()
    info(user, f"iniciando | broker={BROKER} | proxy_sub={PROXY_SUB}")
    do_login()

    while True:
        channels = sync_channels_and_subscriptions()
        target_channel = random.choice(channels)
        info(user, f"iniciando envio de {MESSAGES_PER_BURST} mensagens no canal '{target_channel}'")
        for i in range(1, MESSAGES_PER_BURST + 1):
            text = f"[{i}/{MESSAGES_PER_BURST}] {random.choice(SAMPLE_MESSAGES)}"
            publish_message(target_channel, text)
            time.sleep(PUBLISH_INTERVAL_S)


if __name__ == "__main__":
    main()
