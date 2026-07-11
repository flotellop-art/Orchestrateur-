import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from install_permissions import make_install_plan as make_plan


os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-import-only")

import team  # noqa: E402


class InstallBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_db_path = team.DB_PATH
        team.DB_PATH = Path(self.tmp.name) / "test.db"
        team.install_waiters.clear()
        team.install_waiter_tasks.clear()
        team.running_tasks.clear()
        await team.init_team_db()

    async def asyncTearDown(self):
        for future in list(team.install_waiters.values()):
            if not future.done():
                future.set_result("deny")
        team.install_waiters.clear()
        team.install_waiter_tasks.clear()
        team.running_tasks.clear()
        team.DB_PATH = self.old_db_path
        self.tmp.cleanup()

    async def create_task(self, policy):
        folder = Path(self.tmp.name) / (
            "project-" + policy + "-" + str(len(team.running_tasks) + 1)
        )
        folder.mkdir()
        task_id = await team.db_create_task(
            "test", str(folder), 5, 2, False, "test-model", install_policy=policy
        )
        await team.db_update_task(task_id, status="running")
        pause = asyncio.Event()
        pause.set()
        team.running_tasks[task_id] = {
            "pause": pause,
            "install_lock": asyncio.Lock(),
            "permission_lock": asyncio.Lock(),
            "mcp": {},
        }
        return task_id, folder

    async def wait_for_pending(self, task_id):
        for _ in range(100):
            rows = await team.db_list_install_requests(task_id, status="pending")
            if rows:
                return rows[0]
            await asyncio.sleep(0.01)
        self.fail("La demande d'installation n'est jamais devenue visible.")

    async def test_run_command_cannot_install_directly(self):
        folder = self.tmp.name
        for command in (
            "pip install requests",
            "pip.exe install requests",
            "python -m pip install requests",
            "python -m ensurepip",
            "python -I -m pip install requests",
            "npm install vite",
            "npm.cmd uninstall vite",
            "npm --global install vite",
            "npm ci",
        ):
            with self.subTest(command=command):
                result = await team._run_cmd(folder, command)
                self.assertIn("install_package", result)

    async def test_run_command_resolves_allowed_program_from_trusted_path(self):
        class FakeProcess:
            returncode = 0

            async def communicate(self):
                return b"ok", None

        launcher = AsyncMock(return_value=FakeProcess())
        with (
            patch.object(team.shutil, "which", return_value="C:\\trusted\\node.exe"),
            patch.object(team.asyncio, "create_subprocess_exec", launcher),
        ):
            self.assertEqual(await team._run_cmd(self.tmp.name, "/tmp/evil/node --version"), "ok")
        self.assertEqual(launcher.await_args.args[0], "C:\\trusted\\node.exe")

    def test_npm_bins_never_precede_the_system_path(self):
        folder = Path(self.tmp.name) / "env-test"
        binary_dir = folder / ".orchestrator" / "npm" / "plan" / "node_modules" / ".bin"
        binary_dir.mkdir(parents=True)
        trusted = str(Path(self.tmp.name) / "trusted-system")
        with patch.dict(os.environ, {"PATH": trusted}, clear=False):
            env = team._task_child_environment(folder)
        path_key = next(key for key in env if key.upper() == "PATH")
        self.assertEqual(env[path_key].split(os.pathsep)[0], trusted)

    async def test_unrelated_npm_package_cannot_spoof_official_mcp(self):
        folder = Path(self.tmp.name) / "mcp-spoof"
        fake = folder / ".orchestrator" / "npm" / "other" / "node_modules" / ".bin"
        fake.mkdir(parents=True)
        (fake / ("mcp-server-memory.cmd" if os.name == "nt" else "mcp-server-memory")).write_text(
            "fake", encoding="utf-8"
        )
        (folder / "orchestrator-dependencies.json").write_text(
            json.dumps({"schema": 1, "python": {}, "npm": {"other": "1.0.0"},
                        "applications": {}}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(RuntimeError, "n'est pas installe"):
            await team._ensure_mcp_command("memory", str(folder))

    async def test_project_policy_installs_locally_without_prompt(self):
        task_id, folder = await self.create_task("project")
        action = {
            "manager": "python",
            "package": "requests",
            "version": "2.32.3",
            "reason": "tester un appel HTTP",
        }
        with patch.object(
            team, "_execute_install_plan", AsyncMock(return_value=(True, "ok"))
        ) as execute:
            result = await team._tool_install_package(
                task_id, 2, str(folder), action, "Dev"
            )
        self.assertIn("Installation terminee", result)
        execute.assert_awaited_once()
        self.assertFalse(await team.db_list_install_requests(task_id, status="pending"))
        rows = await team.db_list_install_requests(task_id, status="succeeded")
        self.assertEqual(rows[0]["decision"], "automatic")
        manifest = json.loads(
            (folder / "orchestrator-dependencies.json").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["python"]["requests"], "2.32.3")

    async def test_npm_uses_a_clean_task_owned_environment(self):
        task_id, folder = await self.create_task("project")
        install_plan = make_plan(
            {"manager": "npm", "package": "vite", "version": "5.4.8"},
            platform="win32",
        )
        tools = folder / "tools"
        npm = tools / "npm.cmd"
        node = tools / "node.exe"
        npm_cli = tools / "node_modules" / "npm" / "bin" / "npm-cli.js"
        npm_cli.parent.mkdir(parents=True)
        for executable in (npm, node, npm_cli):
            executable.write_text("", encoding="utf-8")
        runner = AsyncMock(return_value=(True, "ok"))
        with (
            patch.object(
                team.shutil,
                "which",
                side_effect=lambda name: str(node) if name.startswith("node") else str(npm),
            ),
            patch.object(team, "_run_install_process", runner),
        ):
            ok, _ = await team._execute_install_plan(task_id, str(folder), install_plan)
        self.assertTrue(ok)
        args = runner.await_args.args
        argv, cwd = args[1], Path(args[2])
        self.assertEqual(argv[:2], (str(node.resolve()), str(npm_cli.resolve())))
        self.assertIn("--registry=https://registry.npmjs.org/", argv)
        self.assertEqual(argv[argv.index("--prefix") + 1], str(cwd))
        self.assertEqual(cwd.name[:5], ".tmp-")
        self.assertFalse((folder / "node_modules").exists())
        final = folder / ".orchestrator" / "npm" / install_plan.plan_hash
        self.assertTrue(final.is_dir())
        self.assertEqual((final / ".npmrc").read_text(encoding="utf-8"), "")
        self.assertEqual(
            runner.await_args.kwargs["env_overrides"]["NPM_CONFIG_REGISTRY"],
            "https://registry.npmjs.org/",
        )

    async def test_install_process_bounds_output_and_cleans_control_state(self):
        task_id, folder = await self.create_task("project")
        ok, output = await team._run_install_process(
            task_id,
            (sys.executable, "-c", "print('x' * 100000)"),
            str(folder),
            10,
        )
        self.assertTrue(ok)
        self.assertLessEqual(len(output), 4000)
        self.assertNotIn("install_proc", team.running_tasks[task_id])

    async def test_python_install_tools_ignore_shadow_modules_in_project(self):
        task_id, folder = await self.create_task("project")
        marker = folder / "shadow-module-ran"
        shadow_code = (
            "from pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('bad')\n"
        )
        (folder / "venv.py").write_text(shadow_code, encoding="utf-8")
        ok, python_exe = await team._ensure_task_python_venv(task_id, str(folder))
        self.assertTrue(ok, python_exe)
        self.assertFalse(marker.exists())

        (folder / "pip.py").write_text(shadow_code, encoding="utf-8")
        ok, output = await team._run_install_process(
            task_id,
            (python_exe, "-I", "-m", "pip", "--version"),
            str(folder),
            30,
        )
        self.assertTrue(ok, output)
        self.assertFalse(marker.exists())

    async def test_user_install_requires_a_new_decision_each_time(self):
        task_id, folder = await self.create_task("user")
        action = {
            "manager": "winget",
            "package": "Microsoft.VisualStudioCode",
            "version": "1.95.3",
            "scope": "user",
            "reason": "ouvrir les fichiers produits",
        }
        execute = AsyncMock(return_value=(True, "ok"))
        with (
            patch.object(team, "_execute_install_plan", execute),
            patch.object(
                team, "make_install_plan",
                side_effect=lambda payload: make_plan(payload, platform="win32"),
            ),
        ):
            waiting = asyncio.create_task(team._tool_install_package(
                task_id, 3, str(folder), action, "Builder"
            ))
            pending = await self.wait_for_pending(task_id)
            execute.assert_not_awaited()
            response = await team.decide_install_request(
                task_id,
                pending["id"],
                team.InstallDecisionBody(decision="allow_once"),
            )
            self.assertEqual(response["decision"], "allow_once")
            self.assertIn("Installation terminee", await waiting)

            second = asyncio.create_task(team._tool_install_package(
                task_id, 4, str(folder), action, "Builder"
            ))
            second_pending = await self.wait_for_pending(task_id)
            self.assertNotEqual(second_pending["id"], pending["id"])
            self.assertEqual(execute.await_count, 1)
            await team.decide_install_request(
                task_id, second_pending["id"], team.InstallDecisionBody(decision="deny")
            )
            self.assertIn("refusee", (await second).lower())

        self.assertEqual(execute.await_count, 1)
        self.assertFalse(await team.db_list_install_requests(task_id, status="pending"))

    async def test_admin_permission_cannot_be_remembered(self):
        task_id, folder = await self.create_task("admin")
        action = {
            "manager": "winget",
            "package": "Git.Git",
            "version": "2.47.0",
            "scope": "system",
            "reason": "installer Git pour tous les comptes",
        }
        with patch.object(
            team, "make_install_plan",
            side_effect=lambda payload: make_plan(payload, platform="win32"),
        ):
            waiting = asyncio.create_task(team._tool_install_package(
                task_id, 1, str(folder), action, "AdminAgent"
            ))
            pending = await self.wait_for_pending(task_id)
            with self.assertRaises(HTTPException) as caught:
                await team.decide_install_request(
                    task_id,
                    pending["id"],
                    team.InstallDecisionBody(decision="allow_task"),
                )
            self.assertEqual(caught.exception.status_code, 400)
            await team.decide_install_request(
                task_id,
                pending["id"],
                team.InstallDecisionBody(decision="deny"),
            )
            self.assertIn("refusee", (await waiting).lower())

    async def test_request_cannot_be_approved_from_another_task(self):
        task_id, folder = await self.create_task("user")
        other_task_id, _ = await self.create_task("user")
        with patch.object(
            team, "make_install_plan",
            side_effect=lambda payload: make_plan(payload, platform="win32"),
        ):
            waiting = asyncio.create_task(team._tool_install_package(
                task_id,
                1,
                str(folder),
                {
                    "manager": "winget",
                    "package": "Microsoft.PowerToys",
                    "version": "0.86.0",
                    "scope": "user",
                },
                "Agent",
            ))
            pending = await self.wait_for_pending(task_id)
            with self.assertRaises(HTTPException) as caught:
                await team.decide_install_request(
                    other_task_id,
                    pending["id"],
                    team.InstallDecisionBody(decision="allow_once"),
                )
            self.assertEqual(caught.exception.status_code, 404)
            await team.decide_install_request(
                task_id, pending["id"], team.InstallDecisionBody(decision="deny")
            )
            await waiting

    async def test_stop_cancels_pending_request_and_emits_resolution(self):
        task_id, folder = await self.create_task("ask")
        waiting = asyncio.create_task(team._tool_install_package(
            task_id,
            1,
            str(folder),
            {"manager": "python", "package": "requests", "version": "2.32.3"},
            "Agent",
        ))
        pending = await self.wait_for_pending(task_id)
        await team.stop_task(task_id)
        self.assertIn("refusee", (await waiting).lower())
        row = await team.db_get_install_request(pending["id"])
        self.assertEqual(row["status"], "cancelled")
        messages = await team.db_messages_after(task_id, 0)
        self.assertTrue(any(
            message["kind"] == "permission_resolved" and pending["id"] in message["content"]
            for message in messages
        ))

    async def test_expired_request_is_terminal_and_wakes_agent(self):
        task_id, folder = await self.create_task("ask")
        waiting = asyncio.create_task(team._tool_install_package(
            task_id,
            1,
            str(folder),
            {"manager": "python", "package": "requests", "version": "2.32.3"},
            "Agent",
        ))
        pending = await self.wait_for_pending(task_id)
        async with team.aiosqlite.connect(team.DB_PATH) as db:
            await db.execute(
                "UPDATE install_requests SET expires_at=? WHERE id=?",
                ("2000-01-01T00:00:00", pending["id"]),
            )
            await db.commit()
        with self.assertRaises(HTTPException) as caught:
            await team.decide_install_request(
                task_id, pending["id"], team.InstallDecisionBody(decision="allow_once")
            )
        self.assertEqual(caught.exception.status_code, 410)
        self.assertIn("refusee", (await waiting).lower())
        self.assertEqual((await team.db_get_install_request(pending["id"]))["status"], "expired")

    async def test_approval_waits_while_task_is_paused(self):
        task_id, folder = await self.create_task("ask")
        execute = AsyncMock(return_value=(True, "ok"))
        with patch.object(team, "_execute_install_plan", execute):
            waiting = asyncio.create_task(team._tool_install_package(
                task_id,
                1,
                str(folder),
                {"manager": "python", "package": "requests", "version": "2.32.3"},
                "Agent",
            ))
            pending = await self.wait_for_pending(task_id)
            await team.pause_task(task_id)
            await team.decide_install_request(
                task_id, pending["id"], team.InstallDecisionBody(decision="allow_once")
            )
            await asyncio.sleep(0.05)
            execute.assert_not_awaited()
            await team.resume_task(task_id)
            self.assertIn("Installation terminee", await waiting)
        execute.assert_awaited_once()

    async def test_build_never_installs_requirements_silently(self):
        task_id, folder = await self.create_task("blocked")
        (folder / "requirements.txt").write_text("evil-package\n", encoding="utf-8")
        runner = AsyncMock(return_value=(5, ""))
        with patch.object(team, "_proc_run", runner):
            result = await team._run_build(task_id, str(folder))
        self.assertEqual(result["steps"][1]["name"], "dependances autorisees")
        self.assertFalse(result["steps"][1]["ok"])
        flattened = " ".join(
            str(argument)
            for call in runner.await_args_list
            for argument in call.args[0]
        )
        self.assertNotIn("pip", flattened.lower())


if __name__ == "__main__":
    unittest.main()
