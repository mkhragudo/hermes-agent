"""Event-driven wake signal for the gateway-embedded Kanban dispatcher.

Kanban keeps its periodic dispatcher tick as a safety net. Tasks created inside
the gateway wake it through an in-process ``asyncio.Event``. Tasks created by a
CLI or worker subprocess use a private Unix datagram socket owned by the same
dispatcher. Both paths only wake the existing watcher; normal pause, profile,
concurrency, retry, and singleton-lock gates remain authoritative.
"""
from __future__ import annotations

import asyncio
import itertools
import os
import socket
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

_Waker = Tuple[asyncio.AbstractEventLoop, asyncio.Event]
_lock = threading.Lock()
_tokens = itertools.count(1)
_wakers: Dict[int, _Waker] = {}
_SOCKET_NAME = ".dispatcher-wake.sock"


def dispatch_socket_path() -> Path:
    """Return the machine-shared private dispatcher wake socket path."""
    from .kanban_db import kanban_home

    return kanban_home() / "kanban" / _SOCKET_NAME


def register_dispatch_waker(
    loop: asyncio.AbstractEventLoop,
    event: asyncio.Event,
) -> int:
    """Register one in-process dispatcher and return an unregister token."""
    if loop.is_closed():
        raise RuntimeError("cannot register a Kanban dispatch waker on a closed loop")
    token = next(_tokens)
    with _lock:
        _wakers[token] = (loop, event)
    return token


def unregister_dispatch_waker(token: int) -> None:
    """Remove a previously registered in-process dispatcher wake target."""
    with _lock:
        _wakers.pop(int(token), None)


@dataclass
class DispatchSocketReceiver:
    """Event-loop reader for cross-process wake datagrams."""

    loop: asyncio.AbstractEventLoop
    event: asyncio.Event
    sock: socket.socket
    path: Path

    def close(self) -> None:
        try:
            self.loop.remove_reader(self.sock.fileno())
        except Exception:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            pass


def register_cross_process_receiver(
    loop: asyncio.AbstractEventLoop,
    event: asyncio.Event,
) -> DispatchSocketReceiver | None:
    """Bind the private Unix datagram receiver, or return ``None`` if unsupported."""
    if not hasattr(socket, "AF_UNIX") or not hasattr(loop, "add_reader"):
        return None

    path = dispatch_socket_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # Singleton-dispatcher ownership is established before this call, so an
    # existing path is stale from a prior crash/restart and can be replaced.
    path.unlink(missing_ok=True)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        sock.setblocking(False)
        sock.bind(str(path))
        os.chmod(path, 0o600)
    except Exception:
        sock.close()
        path.unlink(missing_ok=True)
        raise

    def _drain() -> None:
        received = False
        while True:
            try:
                sock.recv(64)
            except BlockingIOError:
                break
            except OSError:
                break
            received = True
        if received:
            event.set()

    loop.add_reader(sock.fileno(), _drain)
    return DispatchSocketReceiver(loop=loop, event=event, sock=sock, path=path)


def _request_cross_process_wake() -> bool:
    if not hasattr(socket, "AF_UNIX"):
        return False
    path = dispatch_socket_path()
    if not path.exists():
        return False
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        sock.sendto(b"wake", str(path))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def request_dispatch_wake() -> int:
    """Wake the live dispatcher and return the number of accepted wake paths.

    The in-process event is preferred. When no local watcher is registered, a
    private Unix datagram wakes the gateway from a CLI/worker subprocess. A zero
    return is safe: the periodic dispatcher tick remains the fallback.
    """
    with _lock:
        registrations = list(_wakers.items())

    accepted = 0
    stale: list[int] = []
    for token, (loop, event) in registrations:
        if loop.is_closed():
            stale.append(token)
            continue
        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            stale.append(token)
            continue
        accepted += 1

    if stale:
        with _lock:
            for token in stale:
                _wakers.pop(token, None)

    if accepted == 0 and _request_cross_process_wake():
        accepted = 1
    return accepted


def _reset_for_tests() -> None:
    """Clear process-global registrations between tests."""
    with _lock:
        _wakers.clear()
