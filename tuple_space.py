import threading
import time


class TupleSpace:
    def __init__(self):
        self.store = {}
        self.store_semaphore = threading.Semaphore(1)
        self.stats_semaphore = threading.Semaphore(1)
        self.stats = {
            "total_operations": 0,
            "reads": 0,
            "gets": 0,
            "puts": 0,
            "errors": 0,
            "clients_connected": 0,
        }

    def update_stats(self, key, count=1):
        self.stats_semaphore.acquire()
        self.stats[key] += count
        self.stats_semaphore.release()

    def put(self, key, value):
        self.store_semaphore.acquire()
        self.update_stats("total_operations")
        self.update_stats("puts")

        if key in self.store:
            self.update_stats("errors")
            self.store_semaphore.release()
            return f"ERR {key} already exists"

        self.store[key] = value
        self.store_semaphore.release()
        return f"OK ({key}, {value}) added"

    def read(self, key):
        self.store_semaphore.acquire()
        self.update_stats("total_operations")
        self.update_stats("reads")

        if key not in self.store:
            self.update_stats("errors")
            self.store_semaphore.release()
            return f"ERR {key} does not exist"

        value = self.store[key]
        self.store_semaphore.release()
        return f"OK ({key}, {value}) read"

    def get(self, key):
        self.store_semaphore.acquire()
        self.update_stats("total_operations")
        self.update_stats("gets")

        if key not in self.store:
            self.update_stats("errors")
            self.store_semaphore.release()
            return f"ERR {key} does not exist"

        value = self.store.pop(key)
        self.store_semaphore.release()
        return f"OK ({key}, {value}) removed"

    def log_stats(self):
        while True:
            time.sleep(10)
            self.store_semaphore.acquire()
            self.stats_semaphore.acquire()

            print("\n--- Server Stats ---")
            print("Tuples stored:", len(self.store))
            print("Total operations:", self.stats["total_operations"])
            print("READs:", self.stats["reads"])
            print("GETs:", self.stats["gets"])
            print("PUTs:", self.stats["puts"])
            print("Errors:", self.stats["errors"])
            print("Clients connected:", self.stats["clients_connected"])
            print("-------------------\n")

            self.stats_semaphore.release()
            self.store_semaphore.release()
