import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException
from starlette.requests import Request


os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-import-only")

import orchestrator  # noqa: E402


class GeneratedPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_app_start_cleans_registered_container(self):
        app_id = 7105
        checking = asyncio.Event()
        handle = object()
        state = {"status": "stopped"}

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5015,
            }

        async def db_update(_app_id, **changes):
            state.update(changes)

        async def wait_forever(*_args, **_kwargs):
            checking.set()
            await asyncio.Future()

        runtime = SimpleNamespace(
            launch=AsyncMock(return_value=handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            (temp / "app.py").write_text("print('ok')", encoding="utf-8")
            with (
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(orchestrator, "wait_for_server", side_effect=wait_forever),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                starting = asyncio.create_task(orchestrator.start_app(app_id))
                await checking.wait()
                starting.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await starting

        self.assertEqual(state["status"], "stopped")
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_cancelled_app_start_after_probe_cleans_before_returning(self):
        app_id = 7107
        publishing = asyncio.Event()
        handle = object()
        state = {"status": "stopped"}

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5017,
            }

        async def db_update(_app_id, **changes):
            if changes.get("status") == "running":
                publishing.set()
                await asyncio.Future()
            state.update(changes)

        runtime = SimpleNamespace(
            launch=AsyncMock(return_value=handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            (temp / "app.py").write_text("print('ok')", encoding="utf-8")
            with (
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(
                    orchestrator, "wait_for_server", AsyncMock(return_value=True)
                ),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                starting = asyncio.create_task(orchestrator.start_app(app_id))
                await publishing.wait()
                starting.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await starting

        self.assertEqual(state["status"], "stopped")
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_database_failure_after_probe_cleans_registered_container(self):
        app_id = 7108
        handle = object()
        state = {"status": "stopped"}

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5018,
            }

        async def db_update(_app_id, **changes):
            if changes.get("status") == "running":
                raise OSError("database unavailable")
            state.update(changes)

        runtime = SimpleNamespace(
            launch=AsyncMock(return_value=handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            (temp / "app.py").write_text("print('ok')", encoding="utf-8")
            with (
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(
                    orchestrator, "wait_for_server", AsyncMock(return_value=True)
                ),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                with self.assertRaisesRegex(OSError, "database unavailable"):
                    await orchestrator.start_app(app_id)

        self.assertEqual(state["status"], "stopped")
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_cancelled_creation_stream_cleans_registered_container(self):
        app_id = 7106
        checking = asyncio.Event()
        handle = object()
        state = {"status": "creating"}
        generated = {
            "name": "demo",
            "files": [
                {"filename": "app.py", "content": "print('ok')\n"},
                {"filename": "requirements.txt", "content": "flask\n"},
            ],
        }

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5016,
            }

        async def db_update(_app_id, **changes):
            state.update(changes)

        async def wait_forever(*_args, **_kwargs):
            checking.set()
            await asyncio.Future()

        runtime = SimpleNamespace(
            launch=AsyncMock(return_value=handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)

            async def consume_pipeline():
                return [
                    event
                    async for event in orchestrator.creation_pipeline(
                        app_id, "description", str(temp), 5016
                    )
                ]

            with (
                patch.object(
                    orchestrator,
                    "generate_app_code",
                    AsyncMock(return_value=generated),
                ),
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(orchestrator, "wait_for_server", side_effect=wait_forever),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                creating = asyncio.create_task(consume_pipeline())
                await checking.wait()
                creating.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await creating

        self.assertEqual(state["status"], "stopped")
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_cancelled_creation_after_probe_cleans_before_returning(self):
        app_id = 7110
        publishing = asyncio.Event()
        handle = object()
        state = {"status": "creating"}
        generated = {
            "name": "demo",
            "files": [
                {"filename": "app.py", "content": "print('ok')\n"},
                {"filename": "requirements.txt", "content": "flask\n"},
            ],
        }

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5020,
            }

        async def db_update(_app_id, **changes):
            if changes.get("status") == "running":
                publishing.set()
                await asyncio.Future()
            state.update(changes)

        runtime = SimpleNamespace(
            launch=AsyncMock(return_value=handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)

            async def consume_pipeline():
                return [
                    event
                    async for event in orchestrator.creation_pipeline(
                        app_id, "description", str(temp), 5020
                    )
                ]

            with (
                patch.object(
                    orchestrator,
                    "generate_app_code",
                    AsyncMock(return_value=generated),
                ),
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(
                    orchestrator, "wait_for_server", AsyncMock(return_value=True)
                ),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                creating = asyncio.create_task(consume_pipeline())
                await publishing.wait()
                creating.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await creating

        self.assertEqual(state["status"], "stopped")
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_delete_during_generation_cannot_recreate_an_orphan_app(self):
        app_id = 7104
        generating = asyncio.Event()
        release = asyncio.Event()
        exists = True
        state = {"status": "creating"}
        generated = {
            "name": "demo",
            "files": [
                {"filename": "app.py", "content": "print('ok')\n"},
                {"filename": "requirements.txt", "content": "flask\n"},
            ],
        }

        async def generate(*_args, **_kwargs):
            generating.set()
            await release.wait()
            return generated

        async def db_get(_app_id):
            if not exists:
                return None
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(folder),
                "port": 5014,
            }

        async def db_update(_app_id, **changes):
            if exists:
                state.update(changes)

        async def db_delete(_app_id):
            nonlocal exists
            exists = False

        runtime = SimpleNamespace(
            launch=AsyncMock(),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "generated"

            async def consume_pipeline():
                return [
                    event
                    async for event in orchestrator.creation_pipeline(
                        app_id, "description", str(folder), 5014
                    )
                ]

            with (
                patch.object(orchestrator, "PROJECTS", root),
                patch.object(orchestrator, "generate_app_code", side_effect=generate),
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(orchestrator, "db_delete", side_effect=db_delete),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                creating = asyncio.create_task(consume_pipeline())
                await generating.wait()
                self.assertEqual((await orchestrator.delete_app(app_id))["deleted"], True)
                release.set()
                events = await creating

        self.assertFalse(exists)
        self.assertFalse(folder.exists())
        self.assertTrue(any('"error"' in event for event in events))
        runtime.launch.assert_not_awaited()
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_creation_pipeline_and_stop_share_one_lifecycle_lock(self):
        app_id = 7103
        started = asyncio.Event()
        release = asyncio.Event()
        handle = object()
        state = {"status": "creating"}
        generated = {
            "name": "demo",
            "files": [
                {"filename": "app.py", "content": "print('ok')\n"},
                {"filename": "requirements.txt", "content": "flask\n"},
            ],
        }

        async def launch(*_args, **_kwargs):
            started.set()
            await release.wait()
            return handle

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5013,
            }

        async def db_update(_app_id, **changes):
            state.update(changes)

        runtime = SimpleNamespace(
            launch=AsyncMock(side_effect=launch),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)

            async def consume_pipeline():
                return [
                    event
                    async for event in orchestrator.creation_pipeline(
                        app_id, "description", str(temp), 5013
                    )
                ]

            with (
                patch.object(
                    orchestrator,
                    "generate_app_code",
                    AsyncMock(return_value=generated),
                ),
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(orchestrator, "wait_for_server", AsyncMock(return_value=True)),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                creating = asyncio.create_task(consume_pipeline())
                await started.wait()
                stopping = asyncio.create_task(orchestrator.stop_app(app_id))
                await asyncio.sleep(0)
                self.assertFalse(stopping.done())
                release.set()
                events = await creating
                self.assertEqual((await stopping)["status"], "stopped")

        self.assertTrue(any('"done"' in event for event in events))
        self.assertEqual(state["status"], "stopped")
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.launch.assert_awaited_once()
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_start_and_stop_are_serialized_until_handle_is_registered(self):
        app_id = 7101
        started = asyncio.Event()
        release = asyncio.Event()
        handle = object()
        state = {"status": "stopped"}

        async def launch(*_args, **_kwargs):
            started.set()
            await release.wait()
            return handle

        async def db_get(_app_id):
            return {
                "id": app_id,
                "status": state["status"],
                "folder": str(temp),
                "port": 5011,
            }

        async def db_update(_app_id, **changes):
            state.update(changes)

        runtime = SimpleNamespace(
            launch=AsyncMock(side_effect=launch),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            (temp / "app.py").write_text("print('ok')", encoding="utf-8")
            with (
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(orchestrator, "wait_for_server", AsyncMock(return_value=True)),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                starting = asyncio.create_task(orchestrator.start_app(app_id))
                await started.wait()
                stopping = asyncio.create_task(orchestrator.stop_app(app_id))
                await asyncio.sleep(0)
                self.assertFalse(stopping.done())
                release.set()
                self.assertEqual((await starting)["status"], "running")
                self.assertEqual((await stopping)["status"], "stopped")

        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        self.assertEqual(state["status"], "stopped")
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_two_app_starts_create_only_one_container(self):
        app_id = 7102
        handle = object()
        state = {"status": "stopped"}

        async def db_get(_app_id):
            return {
                "id": app_id,
                "status": state["status"],
                "folder": str(temp),
                "port": 5012,
            }

        async def db_update(_app_id, **changes):
            state.update(changes)

        runtime = SimpleNamespace(
            launch=AsyncMock(return_value=handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers.pop(app_id, None)
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            (temp / "app.py").write_text("print('ok')", encoding="utf-8")
            with (
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(orchestrator, "wait_for_server", AsyncMock(return_value=True)),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                first, second = await asyncio.gather(
                    orchestrator.start_app(app_id), orchestrator.start_app(app_id)
                )

        self.assertEqual(first["status"], "running")
        self.assertEqual(second["status"], "already_running")
        runtime.launch.assert_awaited_once()
        orchestrator.running_servers.pop(app_id, None)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_stale_running_handle_is_cleaned_before_relaunch(self):
        app_id = 7109
        old_handle = object()
        new_handle = object()
        state = {"status": "running"}

        async def db_get(_app_id):
            return {
                "id": app_id,
                "name": "demo",
                "status": state["status"],
                "folder": str(temp),
                "port": 5019,
            }

        async def db_update(_app_id, **changes):
            state.update(changes)

        runtime = SimpleNamespace(
            is_handle_running=AsyncMock(return_value=False),
            launch=AsyncMock(return_value=new_handle),
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )
        orchestrator._app_lifecycle_locks.pop(app_id, None)
        orchestrator.running_servers[app_id] = old_handle
        with tempfile.TemporaryDirectory() as tmp:
            temp = Path(tmp)
            (temp / "app.py").write_text("print('ok')", encoding="utf-8")
            with (
                patch.object(orchestrator, "db_get", side_effect=db_get),
                patch.object(orchestrator, "db_update", side_effect=db_update),
                patch.object(
                    orchestrator, "wait_for_server", AsyncMock(return_value=True)
                ),
                patch.object(
                    orchestrator.team,
                    "sandbox_app_network_allowed",
                    return_value=True,
                ),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
            ):
                result = await orchestrator.start_app(app_id)

        self.assertEqual(result["status"], "running")
        runtime.is_handle_running.assert_awaited_once_with(old_handle)
        runtime.stop_handle.assert_awaited_once_with(old_handle, strict=False)
        runtime.launch.assert_awaited_once()
        self.assertIs(orchestrator.running_servers[app_id], new_handle)
        orchestrator.running_servers.pop(app_id, None)
        orchestrator._app_lifecycle_locks.pop(app_id, None)

    async def test_app_handle_is_removed_only_after_confirmed_cleanup(self):
        app_id = 7001
        handle = object()
        orchestrator.running_servers[app_id] = handle
        runtime = SimpleNamespace(
            stop_handle=AsyncMock(return_value=SimpleNamespace(failed=())),
            stop=AsyncMock(),
            block_execution=Mock(),
        )

        with patch.object(orchestrator.team, "get_sandbox_runtime", return_value=runtime):
            result = await orchestrator._cleanup_running_server(app_id)

        self.assertFalse(result.failed)
        self.assertNotIn(app_id, orchestrator.running_servers)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=False)
        runtime.stop.assert_not_awaited()

    async def test_failed_app_cleanup_keeps_handle_and_blocks_execution(self):
        app_id = 7002
        handle = object()
        orchestrator.running_servers[app_id] = handle
        failure = (("container", "rm refused"),)
        runtime = SimpleNamespace(
            stop_handle=AsyncMock(
                return_value=SimpleNamespace(failed=failure)
            ),
            stop=AsyncMock(),
            block_execution=Mock(),
        )

        with patch.object(orchestrator.team, "get_sandbox_runtime", return_value=runtime):
            result = await orchestrator._cleanup_running_server(app_id)

        self.assertEqual(result.failed, failure)
        self.assertIs(orchestrator.running_servers[app_id], handle)
        runtime.block_execution.assert_called_once()
        orchestrator.running_servers.pop(app_id, None)

    async def test_app_cleanup_recovers_orphans_by_label_without_a_handle(self):
        app_id = 7003
        runtime = SimpleNamespace(
            stop_handle=AsyncMock(),
            stop=AsyncMock(return_value=SimpleNamespace(failed=())),
            block_execution=Mock(),
        )

        with patch.object(orchestrator.team, "get_sandbox_runtime", return_value=runtime):
            await orchestrator._cleanup_running_server(app_id)

        runtime.stop.assert_awaited_once_with("app-7003", strict=False)
        runtime.stop_handle.assert_not_awaited()

    async def test_invalid_generated_path_is_rejected_before_any_write(self):
        malicious = {
            "name": "bad",
            "files": [
                {"filename": "../escape.py", "content": "raise SystemExit"},
                {"filename": "requirements.txt", "content": "flask"},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "generated"
            escaped = root / "escape.py"
            with (
                patch.object(
                    orchestrator,
                    "generate_app_code",
                    AsyncMock(return_value=malicious),
                ),
                patch.object(orchestrator, "db_update", AsyncMock()),
            ):
                events = [
                    event
                    async for event in orchestrator.creation_pipeline(
                        1, "description", str(folder), 3000
                    )
                ]

            self.assertFalse(escaped.exists())
            self.assertFalse(folder.exists())
            self.assertTrue(any('"error"' in event for event in events))

    async def test_health_proves_instance_without_echoing_secret(self):
        secret = "desktop-instance-secret"

        def request_with(value):
            return Request({
                "type": "http",
                "method": "GET",
                "path": "/health",
                "headers": [(b"x-orchestrator-instance", value.encode())],
                "query_string": b"",
                "client": ("127.0.0.1", 1),
                "server": ("127.0.0.1", 8000),
                "scheme": "http",
            })

        with patch.dict(os.environ, {"ORCHESTRATOR_INSTANCE_TOKEN": secret}):
            accepted = await orchestrator.health_check(request_with(secret))
            rejected = await orchestrator.health_check(request_with("wrong"))
        self.assertIs(accepted["instance"], True)
        self.assertIs(rejected["instance"], False)
        self.assertNotIn(secret, repr(accepted))

    async def test_shutdown_hook_requires_ephemeral_desktop_token(self):
        request = Request({
            "type": "http", "method": "POST", "path": "/api/runtime/prepare-shutdown",
            "headers": [(b"x-orchestrator-instance", b"wrong")],
            "query_string": b"", "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 8000), "scheme": "http",
        })
        with patch.dict(os.environ, {"ORCHESTRATOR_INSTANCE_TOKEN": "right"}):
            with self.assertRaises(HTTPException) as raised:
                await orchestrator.prepare_runtime_shutdown(request)
        self.assertEqual(raised.exception.status_code, 404)

    async def test_shutdown_hook_refuses_to_close_when_runtime_cleanup_fails(self):
        request = Request({
            "type": "http", "method": "POST", "path": "/api/runtime/prepare-shutdown",
            "headers": [(b"x-orchestrator-instance", b"right")],
            "query_string": b"", "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 8000), "scheme": "http",
        })
        orchestrator.running_servers.clear()
        runtime = SimpleNamespace(block_execution=Mock())
        with (
            patch.dict(os.environ, {"ORCHESTRATOR_INSTANCE_TOKEN": "right"}),
            patch.object(
                orchestrator.team, "get_sandbox_runtime", return_value=runtime
            ),
            patch.object(
                orchestrator.team,
                "stop_runtime_services",
                AsyncMock(side_effect=orchestrator.team.SandboxError("cleanup failed")),
            ),
        ):
            with self.assertRaises(HTTPException) as raised:
                await orchestrator.prepare_runtime_shutdown(request)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("cleanup failed", str(raised.exception.detail))
        runtime.block_execution.assert_called_once()

    async def test_shutdown_hook_closes_docker_boundary_before_cleanup(self):
        request = Request({
            "type": "http", "method": "POST", "path": "/api/runtime/prepare-shutdown",
            "headers": [(b"x-orchestrator-instance", b"right")],
            "query_string": b"", "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 8000), "scheme": "http",
        })
        events = []
        runtime = SimpleNamespace(
            block_execution=Mock(side_effect=lambda _reason: events.append("block"))
        )

        async def stop_services():
            events.append("cleanup")

        orchestrator.running_servers.clear()
        with (
            patch.dict(os.environ, {"ORCHESTRATOR_INSTANCE_TOKEN": "right"}),
            patch.object(
                orchestrator.team, "get_sandbox_runtime", return_value=runtime
            ),
            patch.object(
                orchestrator.team,
                "stop_runtime_services",
                AsyncMock(side_effect=stop_services),
            ),
        ):
            result = await orchestrator.prepare_runtime_shutdown(request)

        self.assertEqual(result, {"status": "ready"})
        self.assertEqual(events, ["block", "cleanup"])

    async def test_concurrent_shutdown_requests_share_one_cleanup(self):
        request = Request({
            "type": "http", "method": "POST", "path": "/api/runtime/prepare-shutdown",
            "headers": [(b"x-orchestrator-instance", b"right")],
            "query_string": b"", "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 8000), "scheme": "http",
        })
        entered = asyncio.Event()
        release = asyncio.Event()

        async def stop_services():
            entered.set()
            await release.wait()

        runtime = SimpleNamespace(block_execution=Mock())
        orchestrator.running_servers.clear()
        orchestrator._runtime_shutdown_operation = None
        try:
            with (
                patch.dict(os.environ, {"ORCHESTRATOR_INSTANCE_TOKEN": "right"}),
                patch.object(
                    orchestrator.team, "get_sandbox_runtime", return_value=runtime
                ),
                patch.object(
                    orchestrator.team,
                    "stop_runtime_services",
                    AsyncMock(side_effect=stop_services),
                ) as stop,
            ):
                first = asyncio.create_task(
                    orchestrator.prepare_runtime_shutdown(request)
                )
                await entered.wait()
                second = asyncio.create_task(
                    orchestrator.prepare_runtime_shutdown(request)
                )
                await asyncio.sleep(0)
                stop.assert_awaited_once()
                release.set()
                self.assertEqual(await first, {"status": "ready"})
                self.assertEqual(await second, {"status": "ready"})
                stop.assert_awaited_once()
        finally:
            orchestrator._runtime_shutdown_operation = None


if __name__ == "__main__":
    unittest.main()
