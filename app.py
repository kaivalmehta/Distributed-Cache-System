from flask import Flask, render_template, request, jsonify
import subprocess
import socket
import pickle
import time
import signal
import os
import threading
from config import WORKER_NODES, WORKER_PORTS, PRIMARY_SERVER_PORT, REPLICATION_FACTOR
from hashing_ring import ConsistentHashRing
import hashlib

app = Flask(__name__)

# Global process tracking
processes = {
    'primary': None,
    'workers': {}
}
PRIMARY_LOG_PATH = 'primary_server.log'
class SystemManager:
    def __init__(self):
        self.active_nodes = []
        
    def check_node_status(self, node_id):
        if node_id not in WORKER_PORTS:
            return False
        port = WORKER_PORTS[node_id]
        try:
            with socket.create_connection(("localhost", port), timeout=10):
                return True
        except (socket.timeout, ConnectionRefusedError):
            return False
    
    def check_primary_status(self):
        try:
            with socket.create_connection(("localhost", PRIMARY_SERVER_PORT), timeout=1):
                return True
        except (socket.timeout, ConnectionRefusedError):
            return False
    
    def get_system_state(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)
                sock.connect(("localhost", PRIMARY_SERVER_PORT))
                sock.sendall(pickle.dumps({"action": "LIST_KEYS"}))
                data = sock.recv(4096)
                response = pickle.loads(data)
                return response.get("keys", []),response.get("active_nodes",[])
        except Exception as e:
            return [], []
    
    def get_key_details(self, key):
        try:
            with socket.create_connection(("localhost", PRIMARY_SERVER_PORT), timeout=10) as sock:
                sock.sendall(pickle.dumps({
                    "action": "KEY_METADATA",
                    "key": key
                }))
                response = pickle.loads(sock.recv(4096))
                return response
        except Exception as e:
            return {"status": "ERROR", "message": str(e)}
    
    def send_request(self, action, key, value=None):
        request = {"action": action, "key": key}
        if value:
            request["value"] = value
        
        try:
            with socket.create_connection(("localhost", PRIMARY_SERVER_PORT), timeout=10) as sock:
                sock.sendall(pickle.dumps(request))
                data = sock.recv(4096)
                response = pickle.loads(data)
                return response
        except Exception as e:
            return {"status": "ERROR", "message": str(e)}

system_manager = SystemManager()

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/dashboard')
def dashboard():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    primary_status = system_manager.check_primary_status()
    worker_status = {}
    
    for node in WORKER_NODES:
        worker_status[node] = {
            'active': system_manager.check_node_status(node),
            'port': WORKER_PORTS[node],
            'process_running': node in processes['workers'] and 
                            processes['workers'][node] is not None and 
                            processes['workers'][node].poll() is None
        }
    
    keys, active_nodes = system_manager.get_system_state()
    
    return jsonify({
        'primary': {
            'active': primary_status,
            'port': PRIMARY_SERVER_PORT,
            'process_running': processes['primary'] is not None and processes['primary'].poll() is None
        },
        'workers': worker_status,
        'keys_count': len(keys),
        'active_nodes_count': len(active_nodes),
        'replication_factor': REPLICATION_FACTOR
    })

@app.route('/api/start_primary', methods=['POST'])
def start_primary():
    global processes
    
    if processes['primary'] and processes['primary'].poll() is None:
        return jsonify({'success': False, 'message': 'Primary server already running'})
    
    try:
        log_path = PRIMARY_LOG_PATH
        with open(log_path, 'w') as f:
            pass  # clear old logs

        proc = subprocess.Popen(['python3', 'primary_server.py'],
                        stdout=open(log_path, 'a'),
                        stderr=subprocess.STDOUT)

        processes['primary'] = proc
        time.sleep(5)  # Give it time to start
        
        if system_manager.check_primary_status():
            return jsonify({'success': True, 'message': 'Primary server started successfully'})
        else:
            return jsonify({'success': False, 'message': 'Primary server failed to start'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error starting primary: {str(e)}'})

@app.route('/api/stop_primary', methods=['POST'])
def stop_primary():
    global processes
    
    if processes['primary'] and processes['primary'].poll() is None:
        try:
            processes['primary'].terminate()
            processes['primary'].wait(timeout=5)
            processes['primary'] = None
            return jsonify({'success': True, 'message': 'Primary server stopped'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error stopping primary: {str(e)}'})
    else:
        return jsonify({'success': False, 'message': 'Primary server not running'})

@app.route('/api/start_worker', methods=['POST'])
def start_worker():
    global processes
    
    node_id = request.json.get('node_id')
    if not node_id or node_id not in WORKER_NODES:
        return jsonify({'success': False, 'message': 'Invalid node ID'})
    
    if node_id in processes['workers'] and processes['workers'][node_id] and processes['workers'][node_id].poll() is None:
        return jsonify({'success': False, 'message': f'Worker {node_id} already running'})
    
    try:
        proc = subprocess.Popen(['python3', 'worker_node.py', node_id],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        processes['workers'][node_id] = proc
        time.sleep(1)  # Give it time to start
        
        if system_manager.check_node_status(node_id):
            return jsonify({'success': True, 'message': f'Worker {node_id} started successfully'})
        else:
            return jsonify({'success': False, 'message': f'Worker {node_id} failed to start'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error starting worker {node_id}: {str(e)}'})

@app.route('/api/stop_worker', methods=['POST'])
def stop_worker():
    global processes
    
    node_id = request.json.get('node_id')
    if not node_id or node_id not in WORKER_NODES:
        return jsonify({'success': False, 'message': 'Invalid node ID'})
    
    if node_id in processes['workers'] and processes['workers'][node_id] and processes['workers'][node_id].poll() is None:
        try:
            processes['workers'][node_id].terminate()
            processes['workers'][node_id].wait(timeout=5)
            processes['workers'][node_id] = None
            return jsonify({'success': True, 'message': f'Worker {node_id} stopped'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error stopping worker {node_id}: {str(e)}'})
    else:
        return jsonify({'success': False, 'message': f'Worker {node_id} not running'})

@app.route('/api/data', methods=['GET'])
def get_data():
    keys, active_nodes = system_manager.get_system_state()
    print(keys)
    print(active_nodes)
    data_details = []
    
    for key in keys:
        details = system_manager.get_key_details(key)
        if details.get('status') == 'OK':
            key_hash = hashlib.md5(key.encode()).hexdigest()[:8]
            data_details.append({
                'key': key,
                'hash': key_hash,
                'primary': details.get('primary', 'Unknown'),
                'replicas': details.get('replicas', []),
                'value': str(details.get('value', ''))[:50]  # Truncate long values
            })
    
    return jsonify({
        'keys': data_details,
        'active_nodes': active_nodes,
        'total_keys': len(keys)
    })

@app.route('/api/data', methods=['POST'])
def set_data():
    key = request.json.get('key')
    value = request.json.get('value')
    
    if not key or value is None:
        return jsonify({'success': False, 'message': 'Key and value are required'})
    
    response = system_manager.send_request('SET', key, value)
    
    if response.get('status') == 'STORED':
        return jsonify({'success': True, 'message': f'Key "{key}" stored successfully'})
    else:
        return jsonify({'success': False, 'message': f'Failed to store key: {response.get("message", "Unknown error")}'})

@app.route('/api/data/<key>', methods=['GET'])
def get_key(key):
    response = system_manager.send_request('GET', key)
    
    if response.get('status') == 'OK':
        return jsonify({'success': True, 'value': response.get('value')})
    else:
        return jsonify({'success': False, 'message': f'Key not found: {response.get("message", "Unknown error")}'})

@app.route('/api/data/<key>', methods=['DELETE'])
def delete_key(key):
    # Forward DELETE to the primary
    response = system_manager.send_request('DELETE', key)
    if response.get('status') == 'DELETED':
        return jsonify({'success': True, 'message': f'Key "{key}" deleted successfully'})
    else:
        msg = response.get('message', 'Unknown error')
        return jsonify({'success': False, 'message': f'Failed to delete key: {msg}'})

@app.route('/api/simulate_failure', methods=['POST'])
def simulate_failure():
    node_id = request.json.get('node_id')
    if not node_id or node_id not in WORKER_NODES:
        return jsonify({'success': False, 'message': 'Invalid node ID'})
    
    # Stop the worker node to simulate failure
    if node_id in processes['workers'] and processes['workers'][node_id] and processes['workers'][node_id].poll() is None:
        try:
            processes['workers'][node_id].terminate()
            processes['workers'][node_id].wait(timeout=5)
            processes['workers'][node_id] = None
            return jsonify({'success': True, 'message': f'Simulated failure of {node_id}'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error simulating failure: {str(e)}'})
    else:
        return jsonify({'success': False, 'message': f'Worker {node_id} not running'})

@app.route('/api/hash_ring')
def get_hash_ring():
    try:
        keys, active_nodes = system_manager.get_system_state()
        
        # Create ring visualization data
        ring_data = []
        if active_nodes:
            ring = ConsistentHashRing(active_nodes)
            
            # Get virtual nodes for visualization
            for node in active_nodes:
                for i in range(5):  # Show first 5 virtual nodes per node
                    vnode_key = f"{node}-vn{i}"
                    h = int(hashlib.md5(vnode_key.encode()).hexdigest(), 16)
                    ring_data.append({
                        'node': node,
                        'vnode': vnode_key,
                        'hash': str(h)[-8:]  # Last 8 digits
                    })
        
        return jsonify({
            'ring_data': sorted(ring_data, key=lambda x: x['hash']),
            'active_nodes': active_nodes
        })
    except Exception as e:
        return jsonify({'error': str(e)})
@app.route('/api/logs')
def get_logs():
    try:
        with open(PRIMARY_LOG_PATH, 'r') as f:
            lines = f.readlines()[-100:]
        return jsonify({'success': True, 'logs': lines})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    # Cleanup function for graceful shutdown
    import atexit
    
    def cleanup():
        global processes
        print("Cleaning up processes...")
        
        if processes['primary'] and processes['primary'].poll() is None:
            processes['primary'].terminate()
        
        for node_id, proc in processes['workers'].items():
            if proc and proc.poll() is None:
                proc.terminate()
    
    atexit.register(cleanup)
    
    app.run(debug=True, host='0.0.0.0', port=8000)
