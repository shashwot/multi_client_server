import socket
import threading
from tuple_space import TupleSpace

tuple_space = TupleSpace()


def handle_client(conn, addr, tuple_space):
    tuple_space.update_stats("clients_connected")
    print(f"New connection from {addr}")
    conn.settimeout(60)

    with conn:
        try:
            while True:
                data = conn.recv(1024).decode().strip()
                if not data:
                    break

                try:
                    if len(data) < 5:
                        raise ValueError("Malformed request")

                    cmd = data[4]
                    parts = data[5:].split(maxsplit=1)
                    key = parts[0]
                    value = parts[1] if len(parts) > 1 else None

                    if cmd == "P":
                        response = tuple_space.put(key, value)
                    elif cmd == "R":
                        response = tuple_space.read(key)
                    elif cmd == "G":
                        response = tuple_space.get(key)
                    else:
                        response = "ERR Invalid command"

                except Exception as e:
                    tuple_space.update_stats("errors")
                    response = f"ERR {str(e)}"

                formatted_response = f"{len(response):03d} {response}"
                conn.send(formatted_response.encode())

        except socket.timeout:
            print(f"Client {addr} timed out")
        except Exception as e:
            print(f"Error with client {addr}: {e}")
        finally:
            tuple_space.update_stats("clients_connected", -1)
            print(f"Client {addr} disconnected")


def start_server(host, port, max_workers):
    client_semaphore = threading.Semaphore(max_workers)
    threading.Thread(target=tuple_space.log_stats, daemon=True).start()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        print(f"Server listening on {host}:{port}")

        while True:
            conn, addr = s.accept()

            def client_wrapper(conn, addr):
                with client_semaphore:
                    handle_client(conn, addr, tuple_space)

            threading.Thread(target=client_wrapper, args=(conn, addr), daemon=True).start()

