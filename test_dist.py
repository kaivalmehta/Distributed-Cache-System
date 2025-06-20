# test_distribution.py
from hashing_ring import ConsistentHashRing
from config import WORKER_PORTS
import matplotlib.pyplot as plt
import numpy as np

def test_distribution():
    ring = ConsistentHashRing(list(WORKER_PORTS.keys()), vnodes=100)
    test_keys = [f"key_{i}" for i in range(1000)]
    
    distribution = {node: 0 for node in WORKER_PORTS.keys()}
    
    for key in test_keys:
        primary = ring.get_node(key)
        distribution[primary] += 1
    
    # Print stats
    print("\n Distribution Analysis:")
    for node, count in distribution.items():
        print(f"{node}: {count} keys ({count/10}%)")
    
    # Visualize
    plt.bar(distribution.keys(), distribution.values())
    plt.title("Key Distribution Across Nodes")
    plt.ylabel("Number of Keys")
    plt.show()

if __name__ == "__main__":
    test_distribution()
