"""Broker: ROUTER (clientes) <-> DEALER (servidores).

Os clientes (REQ) conectam na porta de frontend; os servidores (REP)
conectam na porta de backend. O DEALER distribui as requisições entre os
servidores disponíveis (round-robin), então novos servidores podem entrar
ou sair sem parar o serviço.
"""
import os

import zmq

from common import info

FRONTEND = os.getenv("FRONTEND_BIND", "tcp://*:5555")
BACKEND = os.getenv("BACKEND_BIND", "tcp://*:5556")
WHO = "BROKER"


def main() -> None:
    ctx = zmq.Context()
    frontend = ctx.socket(zmq.ROUTER)
    frontend.bind(FRONTEND)
    backend = ctx.socket(zmq.DEALER)
    backend.bind(BACKEND)

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)
    info(WHO, f"ativo | clientes em {FRONTEND} | servidores em {BACKEND}")

    n_req = n_rep = 0
    while True:
        events = dict(poller.poll())
        if frontend in events:
            frames = frontend.recv_multipart()
            n_req += 1
            info(WHO, f"requisição #{n_req} encaminhada cliente -> servidor ({sum(map(len, frames))} bytes)")
            backend.send_multipart(frames)
        if backend in events:
            frames = backend.recv_multipart()
            n_rep += 1
            info(WHO, f"resposta    #{n_rep} encaminhada servidor -> cliente ({sum(map(len, frames))} bytes)")
            frontend.send_multipart(frames)


if __name__ == "__main__":
    main()
