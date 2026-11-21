"""Small primary/secondary inventory replication simulation.

The simulation intentionally delays one replication event so a client can read
different values from the two replicas during the consistency window.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Event, Lock, Thread
from time import sleep


@dataclass
class Replica:
    name: str
    _stock: int

    def __post_init__(self) -> None:
        self._lock = Lock()

    def read_stock(self) -> int:
        with self._lock:
            return self._stock

    def write_stock(self, stock: int) -> None:
        with self._lock:
            self._stock = stock


class InventorySystem:
    """Primary accepts writes; a background worker applies them to secondary."""

    def __init__(self, initial_stock: int = 10) -> None:
        self.primary = Replica("Primary", initial_stock)
        self.secondary = Replica("Secondary", initial_stock)
        self._pending: list[tuple[int, float]] = []
        self._pending_lock = Lock()
        self._wake_worker = Event()
        self._stop_worker = Event()
        self._worker = Thread(target=self._replication_worker, daemon=True)
        self._worker.start()

    def update_stock(self, stock: int, replication_delay: float = 0.0) -> None:
        self.primary.write_stock(stock)
        with self._pending_lock:
            self._pending.append((stock, replication_delay))
        self._wake_worker.set()

    def wait_for_convergence(self, expected_stock: int, timeout: float = 3.0) -> bool:
        deadline = __import__("time").monotonic() + timeout
        while __import__("time").monotonic() < deadline:
            if self.secondary.read_stock() == expected_stock:
                return True
            sleep(0.01)
        return False

    def close(self) -> None:
        self._stop_worker.set()
        self._wake_worker.set()
        self._worker.join(timeout=1)

    def _replication_worker(self) -> None:
        while not self._stop_worker.is_set():
            self._wake_worker.wait(timeout=0.05)
            self._wake_worker.clear()
            while True:
                with self._pending_lock:
                    if not self._pending:
                        break
                    stock, delay = self._pending.pop(0)
                if self._stop_worker.wait(timeout=delay):
                    return
                self.secondary.write_stock(stock)
                print(f"[REPLICATION] Secondary received stock={stock}")


def run_demo() -> None:
    system = InventorySystem(initial_stock=10)
    try:
        print("=== Replication Consistency Demo ===")
        print(f"[START] Primary stock={system.primary.read_stock()}")
        print(f"[START] Secondary stock={system.secondary.read_stock()}")

        print("\n[TEST 1] Normal replication")
        system.update_stock(10, replication_delay=0.05)
        sleep(0.10)
        assert system.secondary.read_stock() == 10
        print(f"[READ] Primary={system.primary.read_stock()} Secondary={system.secondary.read_stock()}")
        assert system.primary.read_stock() == system.secondary.read_stock() == 10
        print("[PASS] Replicas return the same value after replication completes.")

        print("\n[TEST 2] Stale read during delayed replication")
        system.update_stock(9, replication_delay=0.50)
        primary_read = system.primary.read_stock()
        secondary_read = system.secondary.read_stock()
        print(f"[READ IMMEDIATELY] Primary={primary_read} Secondary={secondary_read}")
        assert primary_read == 9
        assert secondary_read == 10
        print("[PASS] Secondary temporarily returned stale stock=10 while Primary returned stock=9.")

        print("\n[TEST 3] Recovery and convergence")
        assert system.wait_for_convergence(9)
        print(f"[READ AFTER RECOVERY] Primary={system.primary.read_stock()} Secondary={system.secondary.read_stock()}")
        assert system.primary.read_stock() == system.secondary.read_stock() == 9
        print("[PASS] Replicas converged to stock=9 after replication resumed.")
        print("\n=== ALL TESTS PASSED ===")
    finally:
        system.close()


if __name__ == "__main__":
    run_demo()
