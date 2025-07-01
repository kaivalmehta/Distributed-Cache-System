import socket
import threading
import pickle
import sys
from config import WORKER_PORTS
from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            evicted_key, _ = self.cache.popitem(last=False)
            print(f"[LRU] Evicted key: {evicted_key}")

    def keys(self):
        return list(self.cache.keys())

    def __contains__(self, key):
        return key in self.cache

    def __getitem__(self, key):
        return self.cache[key]

class WorkerNode:
    def __init__(self, node_id, capacity=3):
        self.node_id = node_id
        self.port = WORKER_PORTS[node_id]
        self.cache = LRUCache(capacity)

    def handle_client(self, conn):
        try:
            data = conn.recv(4096)
            if not data:
                return

            command = pickle.loads(data)
            action = command.get("action")
            key = command.get("key")

            if action == "DELETE":

                if key in self.cache:

                    self.cache.cache.pop(key, None)
                    response = {"status": "DELETED"}

                else:

                    response = {"status": "NOT_FOUND"}
                
                conn.sendall(pickle.dumps(response))
                return

            if action == "SET":
                value = command.get("value")
                if key is not None and value is not None:
                    self.cache.put(key, value)
                    response = {"status": "STORED"}
                else:
                    response = {"status": "ERROR", "message": "Missing key or value"}

            elif action == "GET":
                value = self.cache.get(key)
                response = {"status": "OK", "value": value}

            elif action == "KEY_METADATA":
                if key in self.cache:
                    value = self.cache[key]
                    response = {"status": "OK", "value": value}
                else:
                    response = {"status": "NOT_FOUND"}

            elif action == "LIST_KEYS":
                response = {"status": "OK", "keys": self.cache.keys()}

            else:
                response = {"status": "ERROR", "message": f"Unknown action: {action}"}

            conn.sendall(pickle.dumps(response))

        except Exception as e:
            print(f"[{self.node_id}] Error: {e}")
            try:
                conn.sendall(pickle.dumps({"status": "ERROR", "message": str(e)}))
            except:
                pass
        finally:
            conn.close()

    def start(self):
        print(f"[{self.node_id}] Listening on port {self.port}")
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("localhost", self.port))
        server.listen()
        while True:
            conn, _ = server.accept()
            threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    node_id = sys.argv[1]
    node = WorkerNode(node_id)
    node.start()

