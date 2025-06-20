# demo.py
import socket
import pickle
import hashlib
from config import WORKER_PORTS, PRIMARY_SERVER_PORT, REPLICATION_FACTOR
from hashing_ring import ConsistentHashRing
class LiveDemo:
    def __init__(self):
        self.active_nodes = self.check_active_nodes()
        self.ring = self.create_dynamic_ring()

    def check_active_nodes(self):
        active = []
        for node, port in WORKER_PORTS.items():
            try:
                with socket.create_connection(("localhost", port), timeout=0.3):
                    active.append(node)
            except (socket.timeout, ConnectionRefusedError):
                continue
        return active

    def create_dynamic_ring(self):
        return ConsistentHashRing(self.active_nodes)

    def get_all_keys(self):
        try:
            with socket.create_connection(("localhost", PRIMARY_SERVER_PORT), timeout=1) as sock:
                sock.sendall(pickle.dumps({"action": "LIST_KEYS"}))
                return pickle.loads(sock.recv(4096)).get("keys", [])
        except Exception as e:
            print(f"Key retrieval failed: {e}")
            return []

    def get_key_details(self, key):
        try:
            with socket.create_connection(("localhost", PRIMARY_SERVER_PORT), timeout=1) as sock:
                sock.sendall(pickle.dumps({
                    "action": "KEY_METADATA",
                    "key": key
                }))
                return pickle.loads(sock.recv(4096))
        except Exception as e:
            print(f"Metadata error for {key}: {e}")
            return None

    def display(self):
        print("\n")
        print("=" * 85)
        print(f"│ {'Key'.ljust(20)} │ {'Hash'.ljust(12)} │ {'Primary'.ljust(8)} │ {'Replicas'.ljust(20)} │ {'Value'.ljust(15)} │")
        print("=" * 85)
        
        for key in self.get_all_keys():
            details = self.get_key_details(key)
            if not details or details.get("status") != "OK":
                continue
            
            key_hash = hashlib.md5(key.encode()).hexdigest()[:8]
            value_preview = str(details.get("value", "")[:15].replace("\n", " "))
            
            print(f"│ {key.ljust(20)} │ {key_hash}.. │ {details['primary'].ljust(8)} │ {', '.join(details['replicas']).ljust(20)} │ {value_preview.ljust(15)} │")
        
        print("=" * 85)
        print(f"Active Nodes: {len(self.active_nodes)}/{len(WORKER_PORTS)}")
        print(f"Replication Factor: {REPLICATION_FACTOR}")
        print("=" * 85)

if __name__ == "__main__":
    print("\n Key to Node Mappings ")
    demo = LiveDemo()
    demo.display()
