import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-import-only")

import team  # noqa: E402
from automations import AutomationState, AutomationStore, Schedule  # noqa: E402
from durable_queue import DurableQueue, JobState  # noqa: E402


T0 = datetime(2026, 7, 11, 8, 0, tzinfo=timezone.utc)


class TeamDurableRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.projects = self.root / "projects"
        self.projects.mkdir()
        self.previous = {
            "DB_PATH": team.DB_PATH,
            "PROJECTS": team.PROJECTS,
            "task_queue": team.task_queue,
            "automation_store": team.automation_store,
        }
        team.DB_PATH = self.root / "test.db"
        team.PROJECTS = self.projects
        team.task_queue = DurableQueue(team.DB_PATH)
        team.automation_store = AutomationStore(team.DB_PATH)
        await team.init_team_db()
        await team.task_queue.init()
        await team.automation_store.init()

    async def asyncTearDown(self):
        team.running_tasks.clear()
        for key, value in self.previous.items():
            setattr(team, key, value)
        self.temp.cleanup()

    async def _task(self, name: str, *, mode: str = "local", **extra) -> int:
        folder = self.projects / name
        folder.mkdir()
        return await team.db_create_task(
            name,
            str(folder),
            5,
            2,
            False,
            "test-model",
            execution_mode=mode,
            **extra,
        )

    async def test_partial_scheduled_clone_is_completed_once_before_run(self):
        template_id = await self._task("template", mode="docker")
        await team.db_add_agent(
            template_id, "QA", "teste", "claude", "test-model", "user"
        )
        image = self.projects / "template" / "design.png"
        image.write_bytes(b"safe-image")
        await team.db_update_task(template_id, image_path=str(image))

        clone_folder = self.projects / "partial-clone"
        clone_folder.mkdir()
        job = SimpleNamespace(
            id="scheduled-job-1",
            payload={"automation_id": "a" * 32},
        )
        clone_id = await team.db_create_task(
            "partial",
            str(clone_folder),
            5,
            2,
            False,
            "test-model",
            execution_mode="docker",
            source_job_id=job.id,
            source_ready=False,
        )
        await team.db_add_agent(
            clone_id, "incomplet", "a retirer", "claude", "bad", "test"
        )

        recovered_id, initialized = await team._automation_execution_task(
            job, template_id
        )
        self.assertEqual(recovered_id, clone_id)
        self.assertTrue(initialized)
        clone = await team.db_get_task(clone_id)
        self.assertEqual(clone["source_ready"], 1)
        self.assertEqual([a["name"] for a in await team.db_list_agents(clone_id)], ["QA"])
        self.assertEqual(Path(clone["image_path"]).read_bytes(), b"safe-image")

        _, replay_initialized = await team._automation_execution_task(job, template_id)
        self.assertFalse(replay_initialized)
        self.assertEqual(len(await team.db_list_agents(clone_id)), 1)

    async def test_delete_cleans_history_automations_jobs_and_files(self):
        task_id = await self._task("delete-me")
        folder = self.projects / "delete-me"
        (folder / "result.txt").write_text("result", encoding="utf-8")
        async with team.aiosqlite.connect(team.DB_PATH) as db:
            await db.execute(
                "INSERT INTO prompts(task_id,iteration,agent,payload,created_at) "
                "VALUES (?,?,?,?,?)",
                (task_id, 1, "agent", "{}", T0.isoformat()),
            )
            await db.execute(
                "INSERT INTO builds(task_id,status,report,created_at) VALUES (?,?,?,?)",
                (task_id, "success", "[]", T0.isoformat()),
            )
            await db.commit()

        automation = await team.automation_store.create(
            name="delete schedule",
            schedule=Schedule.interval(60, anchor=T0),
            job_kind="run_task",
            payload={"task_id": task_id},
            idempotency_key="delete:schedule",
            now=T0,
        )
        jobs = await team.automation_store.dispatch_due(
            team.task_queue, now=T0 + timedelta(seconds=60)
        )
        self.assertEqual(len(jobs), 1)

        result = await team.delete_task(task_id)
        self.assertEqual(result, {"deleted": True})
        self.assertFalse(folder.exists())
        self.assertIsNone(await team.db_get_task(task_id))
        self.assertEqual(
            (await team.automation_store.get(automation.id)).state,
            AutomationState.CANCELLED,
        )
        self.assertEqual((await team.task_queue.get(jobs[0].id)).state, JobState.CANCELLED)
        db = sqlite3.connect(team.DB_PATH)
        try:
            for table in ("task_agents", "messages", "prompts", "builds", "install_requests"):
                with self.subTest(table=table):
                    count = db.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE task_id=?", (task_id,)
                    ).fetchone()[0]
                    self.assertEqual(count, 0)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
