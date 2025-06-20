# client.py

import socket
import pickle
import argparse
from config import PRIMARY_SERVER_PORT

def send_request(action, key, value=None):
    request = {"action": action, "key": key}
    if value:
        request["value"] = value

    try:
        with socket.create_connection(("localhost", PRIMARY_SERVER_PORT)) as sock:
            sock.sendall(pickle.dumps(request))
            data = sock.recv(4096)
            response = pickle.loads(data)
            return response
    except Exception as e:
        print(f"[Client] Error: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["GET", "SET"])
    parser.add_argument("key", help="The key to fetch or set")
    parser.add_argument("value", nargs="?", help="The value to set (for SET only)")

    args = parser.parse_args()

    if args.action == "SET" and not args.value:
        print("SET requires a value.")
        return

    response = send_request(args.action, args.key, args.value)
    if response:
        print("Response:", response)

if __name__ == "__main__":
    main()

