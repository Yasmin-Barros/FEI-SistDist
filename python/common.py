"""Código compartilhado por broker, servidor e cliente (Python).

Formato das mensagens (serializadas em MessagePack, nunca JSON/texto):

    {
        "service":   str,    # "login" | "channel" | "channels"
        "timestamp": float,  # epoch em segundos, preenchido por quem envia
        "data":      dict,   # conteúdo específico de cada serviço
    }

Respostas usam o mesmo envelope; em `data` sempre existe `status`
("sucesso" ou "erro") e, em caso de erro, `description`.
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


def _fmt_ts(ts) -> str:
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
        f"service={msg.get('service')} | ts_envio={_fmt_ts(msg.get('timestamp'))} | "
        f"data={msg.get('data')}",
        flush=True,
    )


def info(who: str, text: str) -> None:
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] {who:<10} {text}", flush=True)
