"""Código compartilhado por broker, proxy, servidor e cliente (Python).

Formato das mensagens REQ/REP (serializadas em MessagePack, nunca JSON/texto):

    {
        "service":   str,    # "login" | "channel" | "channels" | "publish"
        "timestamp": float,  # epoch em segundos, preenchido por quem envia
        "data":      dict,   # conteúdo específico de cada serviço
    }

Formato das mensagens PUB/SUB (multipart ZeroMQ):
    Frame 0: nome do canal/tópico em UTF-8 (ex.: b"geral") para filtragem nativa do ZMQ
    Frame 1: envelope MessagePack com {"service": "pub", "timestamp": float, "data": {...}}
"""
import re
import time
from datetime import datetime

import msgpack

# Regras de validação (iguais em todas as linguagens do projeto)
USER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
CHANNEL_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")

SUCESSO = "sucesso"
ERRO = "erro"


def pack(service: str, data: dict) -> bytes:
    """Monta o envelope (com timestamp de envio) e serializa em MessagePack."""
    return msgpack.packb(
        {"service": service, "timestamp": time.time(), "data": data},
        use_bin_type=True,
    )


def unpack(raw: bytes) -> dict:
    return msgpack.unpackb(raw, raw=False)


def pack_pub(channel: str, data: dict) -> list[bytes]:
    """Monta mensagem multipart [tópico, envelope MessagePack] para Pub/Sub."""
    payload = pack("pub", {"channel": channel, **data})
    return [channel.encode("utf-8"), payload]


def unpack_pub(frames: list[bytes]) -> tuple[str, dict]:
    """Desempacota mensagem multipart [tópico, envelope MessagePack]."""
    topic = frames[0].decode("utf-8")
    msg = unpack(frames[1])
    return topic, msg


def fmt_ts(ts) -> str:
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S.%f")[:-3]
    except (TypeError, ValueError, OverflowError, OSError):
        return "??"


def log(who: str, direction: str, msg: dict, peer: str = "") -> None:
    """Exibe uma mensagem enviada/recebida com TODO o seu conteúdo."""
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    arrow = "->" if direction == "ENVIADA" else "<-"
    print(
        f"[{now}] {who:<10} {direction:<9} {arrow} {peer:<8} | "
        f"service={msg.get('service')} | ts_envio={fmt_ts(msg.get('timestamp'))} | "
        f"data={msg.get('data')}",
        flush=True,
    )


def info(who: str, text: str) -> None:
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] {who:<10} {text}", flush=True)
