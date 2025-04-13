import socket
import sys
import os
import threading


def send_commands_from_file(host, port, filepath):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            with open(filepath, "r") as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue

                    parts = line.split(maxsplit=2)
                    cmd = parts[0].upper()
                    key = parts[1] if len(parts) > 1 else ""
                    value = parts[2] if len(parts) > 2 else ""

                    if cmd == "PUT":
                        payload = f"P {key} {value}"
                    elif cmd in ("READ", "GET"):
                        payload = f"{cmd[0]} {key}"
                    else:
                        print(f"[{os.path.basename(filepath)}] Invalid command: {line}")
                        continue

                    request = f"{len(payload) + 4:03d} {payload}"

                    try:
                        s.sendall(request.encode())
                        response = s.recv(1024).decode()
                        print(f"[{os.path.basename(filepath)}] {line}: {response[4:]}")
                    except Exception as e:
                        print(
                            f"[{os.path.basename(filepath)}] Error sending request: {e}"
                        )
                        break
    except Exception as e:
        print(f"[{os.path.basename(filepath)}] Connection/File error: {e}")


def process_input_concurrently(host, port, input_path):
    if os.path.isdir(input_path):
        files = sorted(
            [
                os.path.join(input_path, f)
                for f in os.listdir(input_path)
                if os.path.isfile(os.path.join(input_path, f))
            ]
        )
    elif os.path.isfile(input_path):
        files = [input_path]
    else:
        print(f"Error: '{input_path}' is neither a valid file nor folder")
        return

    threads = []

    for file_path in files:
        thread = threading.Thread(
            target=send_commands_from_file, args=(host, port, file_path)
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python client.py <host> <port> <file_or_folder_path>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    input_path = sys.argv[3]

    process_input_concurrently(host, port, input_path)
