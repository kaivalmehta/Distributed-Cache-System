# config.py

# System Configuration
REPLICATION_FACTOR = 2  # Number of replicas per key
VIRTUAL_NODES = 100       # Virtual nodes per worker for consistent hashing
WORKER_NODES = ["node1", "node2", "node3", "node4"]

WORKER_PORTS = {
    "node1": 5001,
    "node2": 5002,
    "node3": 5003,
    "node4": 5004
}

PRIMARY_SERVER_PORT = 4001
