import json
from pathlib import Path


def test_electron_build_does_not_bundle_env_or_database_files():
    package = json.loads(Path("electron-app/package.json").read_text(encoding="utf-8"))
    filters = package["build"]["extraResources"][0]["filter"]

    assert ".env" not in filters
    assert "*.db" not in filters
    assert not any(pattern.endswith(".db") or pattern == ".env" for pattern in filters)


def test_electron_build_still_bundles_runtime_sources_needed_for_first_launch():
    package = json.loads(Path("electron-app/package.json").read_text(encoding="utf-8"))
    filters = package["build"]["extraResources"][0]["filter"]

    assert "orchestrator.py" in filters
    assert "requirements_orchestrator.txt" in filters
    assert "static/**" in filters
