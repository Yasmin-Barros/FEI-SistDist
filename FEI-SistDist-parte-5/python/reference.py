"""Serviço de referência: atribui ranks, lista servidores e monitora heartbeat."""
import os
import time
import threading
import zmq
from common import LamportClock, info, log, pack, unpack

BIND = os.getenv("REFERENCE_BIND", "tcp://*:5559")
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "30"))


def main():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(BIND)
    sock.setsockopt(zmq.RCVTIMEO, 1000)
    clock = LamportClock()
    # name -> {rank, last_seen}; rank permanece estável mesmo após timeout.
    registered = {}
    available = {}
    next_rank = 1
    lock = threading.Lock()
    info("REFERENCE", f"ativo em {BIND}; timeout heartbeat={HEARTBEAT_TIMEOUT}s")

    while True:
        try:
            raw = sock.recv()
        except zmq.Again:
            now = time.time()
            for n, v in list(available.items()):
                if now - v["last_seen"] > HEARTBEAT_TIMEOUT:
                    del available[n]
                    info("REFERENCE", f"servidor '{n}' removido: heartbeat expirado")
            continue
        received_at = time.time()
        # Expira servidores antes de processar qualquer consulta, inclusive list.
        for n, v in list(available.items()):
            if received_at - v["last_seen"] > HEARTBEAT_TIMEOUT:
                del available[n]
                info("REFERENCE", f"servidor '{n}' removido: heartbeat expirado")
        try:
            msg = unpack(raw)
            clock.received(msg.get("logical_clock"))
            log("REFERENCE", "RECEBIDA", msg, "server")
            service = msg.get("service")
            data = msg.get("data") or {}
            name = data.get("name")
            if service == "rank":
                if not isinstance(name, str) or not name:
                    result = {"status": "erro", "description": "nome de servidor inválido"}
                else:
                    if name not in registered:
                        registered[name] = next_rank
                        next_rank += 1
                    available[name] = {"rank": registered[name], "last_seen": received_at}
                    result = {"status": "sucesso", "rank": registered[name]}
            elif service == "list":
                result = {"status": "sucesso", "servers": [
                    {"name": n, "rank": v["rank"]}
                    for n, v in sorted(available.items(), key=lambda item: item[1]["rank"])
                ]}
            elif service == "heartbeat":
                if name not in registered:
                    result = {"status": "erro", "description": "servidor não registrado"}
                else:
                    available[name] = {"rank": registered[name], "last_seen": received_at}
                    result = {"status": "sucesso",
                              "servers": [{"name": n, "rank": v["rank"]}
                                          for n, v in sorted(available.items(), key=lambda x: x[1]["rank"])]}
            else:
                result = {"status": "erro", "description": f"serviço desconhecido: {service}"}
        except Exception as exc:
            service = "erro"
            result = {"status": "erro", "description": f"mensagem inválida: {exc}"}

        reply = pack(str(service), result, clock)
        sock.send(reply)
        log("REFERENCE", "ENVIADA", unpack(reply), "server")


if __name__ == "__main__":
    main()
