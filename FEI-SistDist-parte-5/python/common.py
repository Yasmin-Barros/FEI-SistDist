"""Envelope MessagePack compartilhado. Todas as mensagens incluem timestamp e logical_clock."""
import re
import time
from datetime import datetime
import msgpack

USER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
CHANNEL_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
SUCESSO = "sucesso"
ERRO = "erro"


class LamportClock:
    """Relógio lógico conforme o enunciado: envio incrementa; recepção usa max."""
    def __init__(self):
        self.value = 0

    def sent(self) -> int:
        self.value += 1
        return self.value

    def received(self, remote) -> int:
        if isinstance(remote, int) and remote >= 0:
            self.value = max(self.value, remote)
        return self.value


def pack(service: str, data: dict, clock: LamportClock | None = None,
         timestamp: float | None = None) -> bytes:
    logical = clock.sent() if clock is not None else 0
    return msgpack.packb({
        "service": service,
        "timestamp": time.time() if timestamp is None else timestamp,
        "logical_clock": logical,
        "data": data,
    }, use_bin_type=True)


def unpack(raw: bytes) -> dict:
    return msgpack.unpackb(raw, raw=False)


def pack_pub(channel: str, data: dict, clock: LamportClock | None = None,
             timestamp: float | None = None) -> list[bytes]:
    return [channel.encode("utf-8"), pack("pub", {"channel": channel, **data}, clock, timestamp)]


def unpack_pub(frames: list[bytes]) -> tuple[str, dict]:
    return frames[0].decode("utf-8"), unpack(frames[1])


def fmt_ts(ts) -> str:
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%H:%M:%S.%f")[:-3]
    except (TypeError, ValueError, OverflowError, OSError):
        return "??"


def log(who: str, direction: str, msg: dict, peer: str = "") -> None:
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    arrow = "->" if direction == "ENVIADA" else "<-"
    print(f"[{now}] {who:<10} {direction:<9} {arrow} {peer:<10} | "
          f"service={msg.get('service')} | ts_envio={fmt_ts(msg.get('timestamp'))} | "
          f"logical_clock={msg.get('logical_clock')} | data={msg.get('data')}", flush=True)


def info(who: str, text: str) -> None:
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] {who:<10} {text}", flush=True)
