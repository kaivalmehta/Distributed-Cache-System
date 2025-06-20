import subprocess
import time
import random
import signal
import os
from client import send_request
from config import WORKER_NODES

worker_processes = {}

def start_worker(node_id):
    print(f"[Sim] Starting worker: {node_id}")
    proc = subprocess.Popen(["python3", "worker_node.py", node_id])
    worker_processes[node_id] = proc
    time.sleep(1)  

def kill_worker(node_id):
    print(f"[Sim] Simulating failure of: {node_id}")
    proc = worker_processes.get(node_id)
    if proc:
        proc.terminate()
        proc.wait()
        print(f"[Sim] {node_id} has been terminated.")

def start_primary():
    print("[Sim] Starting primary server...")
    return subprocess.Popen(["python3", "primary_server.py"])

def insert_sample_data():
    keys = ["user:101", "user:102", "user:103", "user:104"]
    values = ["Nishil", "Denil", "Kaival", "Aaryan"]

    for k, v in zip(keys, values):
        print(f"[Sim] SET {k} → {v}")
        send_request("SET", k, v)

def fetch_all_keys():
    keys = ["user:101", "user:102", "user:103", "user:104"]
    for k in keys:
        resp = send_request("GET", k)
        val = resp.get("value") if resp else None
        print(f"[Sim] GET {k} ->{val if val else ' Not Found'}")

if __name__ == "__main__":
    try:
        #  Start worker nodes
        for node in WORKER_NODES:
            start_worker(node)

        #  Start primary server
        primary_proc = start_primary()
        time.sleep(2)  # Let it initialize

        #  Insert data
        insert_sample_data()

        print("\n Data inserted. Fetching before failure...")
        fetch_all_keys()

        # Step 4: Kill a random node
        failed_node = "node3"
        kill_worker(failed_node)

        print(f"\n {failed_node} failed. Fetching data after failure...")
        fetch_all_keys()

    finally:
        print("\n Cleaning up...")
        for proc in worker_processes.values():
            if proc.poll() is None:
                proc.terminate()
        if primary_proc and primary_proc.poll() is None:
            primary_proc.terminate()
