import socket
import threading
import pickle
import time
from config import PRIMARY_SERVER_PORT, REPLICATION_FACTOR, WORKER_PORTS
from hashing_ring import ConsistentHashRing
from replicator import replicate
from datastore import DataStore
import functools
print = functools.partial(print, flush=True)


class PrimaryCacheServer:
    def __init__(self):
        self.datastore = DataStore()
        self.hash_ring = self.create_hash_ring_with_active_nodes()
        threading.Thread(target=self.node_monitor_loop, daemon=True).start()

    def create_hash_ring_with_active_nodes(self):
        active_nodes = []
        for node, port in WORKER_PORTS.items():
            if self.is_node_active(port):
                active_nodes.append(node)
        print(f"Active nodes detected: {active_nodes}")
        return ConsistentHashRing(active_nodes)

    def is_node_active(self, port):
        try:
            with socket.create_connection(("localhost", port), timeout=10):
                return True
        except (socket.timeout, ConnectionRefusedError):
            return False

    def forward_request(self, node, message):
        port = WORKER_PORTS[node]
        try:
            with socket.create_connection(("localhost", port)) as sock:
                sock.sendall(pickle.dumps(message))
                data = sock.recv(4096)
                return pickle.loads(data)
        except Exception as e:
            print(f"[PrimaryServer] Error contacting {node}: {e}")
            return {"status": "ERROR"}

    def fetch_value_from_node(self, node, key):
        port = WORKER_PORTS[node]
        with socket.create_connection(("localhost", port)) as s:
            s.sendall(pickle.dumps({"action": "GET", "key": key}))
            response = pickle.loads(s.recv(4096))
            if response["status"] == "OK":
                return response["value"]
            raise Exception("Fetch failed")

    def redistribute_keys_from_failed_node(self, failed_node):
        print(f"\n[Redistribution] Starting for failed node {failed_node}")

        affected_keys = [
            key for key in self.datastore.store.keys()
            if self.hash_ring.get_node(key) == failed_node
        ]
    
        if not affected_keys:
            #print("No keys need redistribution")
            return
    
        #print(f"Affected keys: {len(affected_keys)}")
    
        for key in affected_keys:
            try:
                replicas = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)
                live_replicas = [
                    node for node in replicas 
                    if node != failed_node and self.is_node_active(WORKER_PORTS[node])
                ]
            
                if not live_replicas:
                    print(f" No live replicas for key: {key}, checking if value exists in local DataStore")
                    value = self.datastore.get(key)
                    if value:
                        new_replicas = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)
                        replicate(key, value, new_replicas)
                        print(f" {key} redistributed from local -> {new_replicas}")
                    else:
                        print(f" No live replicas or local value for key: {key}")
                    continue
                
                source_node = live_replicas[0]
                value = self.fetch_value_from_node(source_node, key)
                new_replicas = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)
                self.datastore.set(key, value)
                replicate(key, value, new_replicas)
                print(f"{key} redistributed via {source_node} -> {new_replicas}")
            
            except Exception as e:
                pass

    def node_monitor_loop(self):
        while True:
            alive_nodes = []
            for node in WORKER_PORTS.keys():
                if self.is_node_active(WORKER_PORTS[node]):
                    alive_nodes.append(node)

            current_nodes = set(self.hash_ring.nodes)
            detected_nodes = set(alive_nodes)

            removed = current_nodes - detected_nodes
            added = detected_nodes - current_nodes

            for node in removed:
                print(f"[Monitor] Detected failure of node: {node}")
                self.hash_ring.remove_node(node)
                self.redistribute_keys_from_failed_node(node)

            for node in added:
                print(f"[Monitor] Detected recovered/new node: {node}")
                self.hash_ring.add_node(node)

            time.sleep(3)

    def handle_client(self, conn):
        try:
            data = conn.recv(4096)
            if not data:
                return
            try:
                request = pickle.loads(data)
            except pickle.UnpicklingError:
                return
            action = request.get("action")
            if action in ("GET", "SET", "KEY_METADATA", "DELETE"):
                key = request.get("key")

            if action == "DELETE":
                removed = self.datastore.store.pop(key, None) is not None
                replicas = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)

                for node in replicas:

                    try:

                        with socket.create_connection(("localhost", WORKER_PORTS[node])) as sock:
                            sock.sendall(pickle.dumps({"action": "DELETE", "key": key}))
                    except:
                        pass
                
                status = "DELETED" if removed else "MISS"
                conn.sendall(pickle.dumps({"status": status, "message": removed and "" or "Key not found"}))
                return

            if action == "GET":
                nodes = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)
                print(f"[PrimaryServer]  GET request for '{key}' -> checking replicas: {nodes}")
               
                for node in nodes:
                    print(f"[PrimaryServer] -> Trying {node}...")
                    response = self.forward_request(node, request)
                    if response.get("value") is not None:
                        print(f"[PrimaryServer]  Found value in {node}: {response['value']}")
                        conn.sendall(pickle.dumps(response))
                        return
                    else:
                        print(f"[PrimaryServer]  {node} did not return value.")

                # Fallback: check original DataStore
                value = self.datastore.get(key)
                if value:
                    print(f"[PrimaryServer] Cache miss  found in DataStore. Replicating to {nodes}")
                    replicate(key, value, nodes)
                    conn.sendall(pickle.dumps({"status": "OK", "value": value}))
                else:
                    print(f"[PrimaryServer] Key '{key}' not found anywhere.")
                    conn.sendall(pickle.dumps({"status": "MISS"}))
            elif action == "SET":
                self.datastore.set(key, request["value"])
                nodes = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)
                replicate(key, request["value"], nodes)
                conn.sendall(pickle.dumps({"status": "STORED"}))

            elif action == "LIST_KEYS":
                all_keys = list(self.datastore.store.keys())
                active_nodes = list(self.hash_ring.nodes)
                for node in self.hash_ring.nodes:
                    try:
                        with socket.create_connection(("localhost", WORKER_PORTS[node])) as sock:
                            sock.sendall(pickle.dumps({"action": "LIST_KEYS"}))
                            data = sock.recv(4096)
                            if data:
                                worker_keys = pickle.loads(data).get("keys", [])
                                all_keys.extend(worker_keys)
                    except:
                        continue
                conn.sendall(pickle.dumps({"status": "OK", "keys": list(set(all_keys)), "active_nodes": active_nodes}))

            elif action == "KEY_METADATA":
                key = request["key"]
                nodes = self.hash_ring.get_replicas(key, REPLICATION_FACTOR)
                primary = nodes[0] if nodes else "None"
                conn.sendall(pickle.dumps({
                    "status": "OK",
                    "primary": primary,
                    "replicas": nodes,
                    "value": self.datastore.get(key) or "In worker cache"
                }))

            else:
                conn.sendall(pickle.dumps({"status": "ERROR", "message": "Unknown action"}))
        finally:
            conn.close()

    def start(self):
        print("[PrimaryServer] Starting on port", PRIMARY_SERVER_PORT)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("localhost", PRIMARY_SERVER_PORT))
        server.listen()

        while True:
            conn, _ = server.accept()
            threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    primary_server = PrimaryCacheServer()
    primary_server.start()
