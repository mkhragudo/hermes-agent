from __future__ import annotations

import asyncio
import importlib
import os
import socket

import pytest


def _signal_module():
    # Some broader Hermes suites intentionally reset/re-import plugin/runtime
    # modules. Resolve the live module per test so the registration and the
    # kanban_create handler always share the same process-global registry.
    return importlib.import_module("hermes_cli.kanban_dispatch_signal")


def test_request_dispatch_wake_reaches_registered_loop_from_worker_thread():
    signal = _signal_module()
    async def scenario() -> None:
        signal._reset_for_tests()
        event = asyncio.Event()
        token = signal.register_dispatch_waker(asyncio.get_running_loop(), event)
        try:
            accepted = await asyncio.to_thread(signal.request_dispatch_wake)
            assert accepted == 1
            await asyncio.wait_for(event.wait(), timeout=0.5)
        finally:
            signal.unregister_dispatch_waker(token)
            signal._reset_for_tests()

    asyncio.run(scenario())


def test_request_dispatch_wake_without_gateway_is_safe_noop():
    signal = _signal_module()
    signal._reset_for_tests()
    assert signal.request_dispatch_wake() == 0


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix socket required")
def test_cross_process_socket_wakes_dispatcher(monkeypatch, tmp_path):
    signal = _signal_module()
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(tmp_path))

    async def scenario() -> None:
        signal._reset_for_tests()
        event = asyncio.Event()
        receiver = signal.register_cross_process_receiver(
            asyncio.get_running_loop(), event,
        )
        assert receiver is not None
        try:
            assert os.stat(receiver.path).st_mode & 0o777 == 0o600
            accepted = await asyncio.to_thread(signal.request_dispatch_wake)
            assert accepted == 1
            await asyncio.wait_for(event.wait(), timeout=0.5)
        finally:
            receiver.close()
            signal._reset_for_tests()

    asyncio.run(scenario())


def test_tool_create_wake_drives_same_process_dispatch(monkeypatch, tmp_path):
    signal = _signal_module()
    from pathlib import Path

    from hermes_cli import kanban_db as kb
    from hermes_cli import profiles
    from tools import kanban_tools

    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE", "atlasea")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(profiles, "profile_exists", lambda name: name == "default")
    kb._INITIALIZED_PATHS.clear()
    kb.init_db()

    async def scenario() -> None:
        signal._reset_for_tests()
        event = asyncio.Event()
        token = signal.register_dispatch_waker(asyncio.get_running_loop(), event)
        try:
            raw = await asyncio.to_thread(
                kanban_tools._handle_create,
                {
                    "title": "EA event-driven canary",
                    "assignee": "default",
                    "board": "default",
                    "triage": False,
                },
            )
            import json

            created = json.loads(raw)
            assert created["ok"] is True
            assert created["status"] == "ready"
            assert created["dispatch_wake_requested"] is True
            await asyncio.wait_for(event.wait(), timeout=0.5)

            conn = kb.connect(board="default")
            try:
                result = kb.dispatch_once(
                    conn,
                    board="default",
                    spawn_fn=lambda *args: 4242,
                )
                task = kb.get_task(conn, created["task_id"])
            finally:
                conn.close()

            assert [row[0] for row in result.spawned] == [created["task_id"]]
            assert task is not None
            assert task.status == "running"
            assert task.worker_pid == 4242
        finally:
            signal.unregister_dispatch_waker(token)
            signal._reset_for_tests()

    asyncio.run(scenario())
