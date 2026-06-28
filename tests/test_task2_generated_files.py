import asyncio
from pathlib import Path

import orchestrator


VALID_APP = """
from flask import Flask
app = Flask(__name__)

@app.route('/')
def index():
    return 'ok'

if __name__ == '__main__':
    app.run(port=3456)
"""


def run_pipeline(monkeypatch, tmp_path, files):
    updates = []

    async def fake_generate_app_code(description, port, last_error=""):
        return {"name": "safe-test", "files": files}

    async def fake_db_update(app_id, **kwargs):
        updates.append(kwargs)

    monkeypatch.setattr(orchestrator, "generate_app_code", fake_generate_app_code)
    monkeypatch.setattr(orchestrator, "db_update", fake_db_update)

    async def collect():
        events = []
        folder = tmp_path / "project"
        async for event in orchestrator.creation_pipeline(1, "demo", str(folder), 3456):
            events.append(event)
        return events, updates, folder

    return asyncio.run(collect())


def test_safe_generated_file_path_allows_only_expected_names(tmp_path):
    target = tmp_path / "project"
    target.mkdir()

    assert orchestrator.resolve_generated_file_path(target, "app.py") == (target / "app.py").resolve()
    assert orchestrator.resolve_generated_file_path(target, "requirements.txt") == (target / "requirements.txt").resolve()


async def _unexpected_subprocess(*args, **kwargs):  # pragma: no cover - assertion helper
    raise AssertionError("Le pipeline ne doit pas lancer pip/app quand un fichier est refuse")


def test_generated_parent_traversal_is_rejected_without_writing_outside(monkeypatch, tmp_path):
    outside = tmp_path / "evil.py"
    assert not outside.exists()

    events, updates, folder = run_pipeline(
        monkeypatch,
        tmp_path,
        [
            {"filename": "app.py", "content": VALID_APP},
            {"filename": "../evil.py", "content": "pwned"},
        ],
    )

    assert any(update.get("status") == "failed" for update in updates)
    assert any("Fichier genere refuse" in event for event in events)
    assert not outside.exists()
    assert not (folder / "app.py").exists()


def test_generated_absolute_path_is_rejected_without_writing(monkeypatch, tmp_path):
    written_paths = []
    original_write_text = Path.write_text

    def spy_write_text(self, *args, **kwargs):
        written_paths.append(self)
        return original_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", spy_write_text)

    events, updates, _folder = run_pipeline(
        monkeypatch,
        tmp_path,
        [
            {"filename": "app.py", "content": VALID_APP},
            {"filename": "/etc/passwd", "content": "pwned"},
        ],
    )

    assert any(update.get("status") == "failed" for update in updates)
    assert any("Fichier genere refuse" in event for event in events)
    assert written_paths == []
