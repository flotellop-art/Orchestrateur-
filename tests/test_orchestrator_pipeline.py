import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request


os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-import-only")

import orchestrator  # noqa: E402


class GeneratedPipelineTests(unittest.IsolatedAsyncioTestCase):
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


if __name__ == "__main__":
    unittest.main()
