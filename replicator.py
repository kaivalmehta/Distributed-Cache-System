# replicator.py

import socket
import pickle
from config import WORKER_PORTS

def replicate(key, value, nodes):
    for node in nodes:
        port = WORKER_PORTS[node]
        try:
            with socket.create_connection(("localhost", port)) as sock:
                msg = {"action": "SET", "key": key, "value": value}
                sock.sendall(pickle.dumps(msg))
        except Exception as e:
            print(f"[Replicator] Failed to replicate to {node}: {e}")

def replicate_delete(key, nodes):
    for node in nodes:
        port = WORKER_PORTS[node]
        
        try:
            with socket.create_connection(("localhost", port)) as sock:
                sock.sendall(pickle.dumps({"action": "DELETE", "key": key}))

        except Exception as e:
            print(f"[Replicator] Failed to delete on {node}: {e}")

