# ring_visual.py

import hashlib
from config import WORKER_NODES, VIRTUAL_NODES
from hashing_ring import ConsistentHashRing
import socket
import pickle

PRIMARY_HOST = "localhost"
PRIMARY_PORT = 4001

def fetch_system_state():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((PRIMARY_HOST, PRIMARY_PORT))
            sock.sendall(pickle.dumps({"action": "LIST_KEYS"}))
            data = sock.recv(4096)
            response = pickle.loads(data)
            return response.get("keys", []), response.get("active_nodes", [])
    except Exception as e:
        print(f"[Error] Could not fetch system state from primary server: {e}")
        return [], []

def hash_key(key):
    return int(hashlib.md5(key.encode()).hexdigest(), 16)

def get_virtual_nodes_map(nodes, vnodes):
    ring_map = {}
    for node in nodes:
        ring_map[node] = []
        for i in range(5):
            vnode_key = f"{node}-vn{i}"
            h = hash_key(vnode_key)
            ring_map[node].append((vnode_key, h))
    return ring_map

def show_ring_adj_list(nodes):
    print("\n HASH RING VIRTUAL NODES (Adjacency List Style)\n")
    ring_map = get_virtual_nodes_map(nodes, VIRTUAL_NODES)

    for node in sorted(ring_map):
        print(f"{node}:")
        for vnode, h in sorted(ring_map[node], key=lambda x: x[1]):
            print(f"  ↳ {vnode} -> hash: {str(h)[-6:]}")
        print()


if __name__ == "__main__":
    keys, active_nodes = fetch_system_state()

    if not active_nodes:
        print(" No active nodes found. Is the primary server running?")
    else:
        show_ring_adj_list(active_nodes)


