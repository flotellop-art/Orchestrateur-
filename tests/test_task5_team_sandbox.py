import asyncio

import team
from patches.security.sandbox_commands import is_command_allowed


def test_team_run_cmd_refuses_dangerous_commands(tmp_path):
    for cmd in [
        "pip install requests",
        "npm install left-pad",
        "python -c 'import os; print(os.getcwd())'",
        "curl https://example.com",
    ]:
        result = asyncio.run(team._run_cmd(str(tmp_path), cmd))
        assert "Commande refusee" in result, cmd


def test_sandbox_still_allows_pytest_and_ls_commands():
    assert is_command_allowed("pytest")[0]
    assert is_command_allowed("ls")[0]


def test_team_run_cmd_delegates_allowed_commands_to_sandbox(monkeypatch, tmp_path):
    calls = []

    async def fake_safe_run_command(cmd, cwd=None, timeout=30.0):
        calls.append((cmd, cwd, timeout))
        return 0, "ok", ""

    monkeypatch.setattr(team, "safe_run_command", fake_safe_run_command)

    assert asyncio.run(team._run_cmd(str(tmp_path), "pytest", timeout=12)) == "ok"
    assert asyncio.run(team._run_cmd(str(tmp_path), "ls", timeout=12)) == "ok"
    assert calls == [
        ("pytest", str(tmp_path), 12),
        ("ls", str(tmp_path), 12),
    ]


def test_team_run_cmd_executes_ls_without_shell_alias_dependency(tmp_path):
    (tmp_path / "visible.txt").write_text("ok", encoding="utf-8")

    result = asyncio.run(team._run_cmd(str(tmp_path), "ls"))

    assert "Commande refusee" not in result
    assert "Executable introuvable" not in result
    assert "visible.txt" in result
