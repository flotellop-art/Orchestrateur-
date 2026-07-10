import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


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


if __name__ == "__main__":
    unittest.main()
