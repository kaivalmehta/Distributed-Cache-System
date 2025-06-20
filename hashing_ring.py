# hashing_ring.py

import hashlib
import bisect
from config import WORKER_NODES, VIRTUAL_NODES, WORKER_PORTS, REPLICATION_FACTOR

        
class ConsistentHashRing:
    def __init__(self, nodes=None, vnodes=100):
        self.ring = {}
        self.sorted_keys = []
        self.vnodes = vnodes
        self.nodes = nodes or []

        for node in self.nodes:
            self.add_node(node)

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.vnodes):
            vnode_key = f"{node}-vn{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node
            bisect.insort(self.sorted_keys, h)

    def remove_node(self, node):
        keys_to_remove = [h for h, n in self.ring.items() if n == node]
        for h in keys_to_remove:
            try:
                self.ring.pop(h)
                self.sorted_keys.remove(h)
            except ValueError:
                continue  
        self.nodes = [n for n in self.nodes if n != node]
        print(f"Removed node {node} (cleaned {len(keys_to_remove)} vnodes)")


    def get_node(self, key):
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]

    def get_replicas(self, key, count=2):
        if not self.sorted_keys:
            return []

        h = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, h)
        replicas = set()
        active_nodes = set(self.nodes)  
        attempts = 0  
        while len(replicas) < min(count, len(active_nodes)) and attempts < len(self.sorted_keys):
            node = self.ring[self.sorted_keys[idx % len(self.sorted_keys)]]
            if node in active_nodes:
                replicas.add(node)
            idx += 1
            attempts += 1

        return list(replicas)

    
        
    

# if __name__ == "__main__":
#     import sys
#     ring = ConsistentHashRing(WORKER_PORTS.keys())
#     keys = sys.argv[1:]
#     for key in keys:
#         primary = ring.get_node(key)
#         replicas = ring.get_replicas(key, REPLICATION_FACTOR)
#         print(f"Key '{key}' -> Primary: {primary}, Replicas: {replicas}")
    
    

