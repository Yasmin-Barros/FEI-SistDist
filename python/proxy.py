"""Proxy Pub/Sub: XSUB (servidores publicam) <-> XPUB (clientes assinam).

Os servidores (PUB) conectam na porta 5557 (XSUB) e os clientes (SUB)
conectam na porta 5558 (XPUB). O zmq.proxy encaminha mensagens e propaga
as inscrições de tópicos automaticamente.
"""
import os

import zmq

from common import info

XSUB_BIND = os.getenv("XSUB_BIND", "tcp://*:5557")
XPUB_BIND = os.getenv("XPUB_BIND", "tcp://*:5558")
WHO = "PROXY"


def main() -> None:
    ctx = zmq.Context()
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(XSUB_BIND)

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(XPUB_BIND)

    info(WHO, f"ativo | XSUB (publishers) em {XSUB_BIND} | XPUB (subscribers) em {XPUB_BIND}")
    zmq.proxy(xsub, xpub)


if __name__ == "__main__":
    main()
