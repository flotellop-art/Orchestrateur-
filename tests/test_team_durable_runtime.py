import asyncio
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-import-only")

import team  # noqa: E402
import automation_api  # noqa: E402
from automations import AutomationState, AutomationStore, Schedule  # noqa: E402
from durable_queue import DurableQueue, JobState, LeaseLost  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from sandbox_runtime import SandboxStopError, SandboxStopResult  # noqa: E402


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
            "_runtime_service_error": team._runtime_service_error,
        }
        team.DB_PATH = self.root / "test.db"
        team.PROJECTS = self.projects
        team.task_queue = DurableQueue(team.DB_PATH)
        team.automation_store = AutomationStore(team.DB_PATH)
        team._runtime_service_error = None
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

    async def test_stop_launched_keeps_handle_when_docker_cleanup_fails(self):
        task_id = 9001
        handle = object()
        info = {"sandbox": handle, "url": "http://127.0.0.1:5000"}
        team.launched_apps[task_id] = info
        failure = SandboxStopError(
            SandboxStopResult("9001", (), (("container", "rm refused"),))
        )
        runtime = SimpleNamespace(stop_handle=AsyncMock(side_effect=failure))

        with patch.object(team, "get_sandbox_runtime", return_value=runtime):
            with self.assertRaises(SandboxStopError):
                await team._stop_launched(task_id)

        self.assertIs(team.launched_apps[task_id], info)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=True)
        team.launched_apps.pop(task_id, None)

    async def test_successful_runtime_shutdown_closes_docker_until_restart(self):
        previous_runtime = team.sandbox_runtime
        previous_shutdown = team._runtime_shutdown
        previous_background = team._runtime_background_tasks
        previous_launched = dict(team.launched_apps)
        events = []

        async def stop_all(*, strict):
            self.assertFalse(strict)
            events.append("stop")
            return SandboxStopResult("*", (), ())

        async def stop_handle(_handle, *, strict):
            self.assertTrue(strict)
            events.append("preview")

        runtime = SimpleNamespace(
            stop_all_managed=AsyncMock(side_effect=stop_all),
            stop_handle=AsyncMock(side_effect=stop_handle),
            block_execution=Mock(side_effect=lambda _reason: events.append("block")),
        )
        team.sandbox_runtime = runtime
        team._runtime_shutdown = asyncio.Event()
        team._runtime_background_tasks = []
        team.launched_apps.clear()
        team.launched_apps[99] = {"sandbox": object()}
        try:
            await team.stop_runtime_services()
        finally:
            team.sandbox_runtime = previous_runtime
            team._runtime_shutdown = previous_shutdown
            team._runtime_background_tasks = previous_background
            team.launched_apps.clear()
            team.launched_apps.update(previous_launched)

        runtime.stop_all_managed.assert_awaited_once_with(strict=False)
        runtime.block_execution.assert_called_once()
        self.assertEqual(events, ["block", "preview", "stop"])

    async def test_failed_runtime_shutdown_keeps_handles_and_restarts_services(self):
        previous_runtime = team.sandbox_runtime
        previous_shutdown = team._runtime_shutdown
        previous_background = team._runtime_background_tasks
        previous_launched = dict(team.launched_apps)
        failed = SandboxStopResult("*", (), (("container", "rm refused"),))
        runtime = SimpleNamespace(
            stop_all_managed=AsyncMock(return_value=failed),
            stop_handle=AsyncMock(side_effect=SandboxStopError(failed)),
            block_execution=Mock(),
        )
        handle = object()
        team.sandbox_runtime = runtime
        team._runtime_shutdown = asyncio.Event()
        team._runtime_background_tasks = []
        team.launched_apps.clear()
        team.launched_apps[100] = {"sandbox": handle}
        try:
            with patch.object(
                team, "start_runtime_services", new=AsyncMock()
            ) as restart:
                with self.assertRaises(SandboxStopError):
                    await team.stop_runtime_services()
                restart.assert_awaited_once()
            self.assertIs(team.launched_apps[100]["sandbox"], handle)
        finally:
            team.sandbox_runtime = previous_runtime
            team._runtime_shutdown = previous_shutdown
            team._runtime_background_tasks = previous_background
            team.launched_apps.clear()
            team.launched_apps.update(previous_launched)

        self.assertEqual(runtime.block_execution.call_count, 2)
        self.assertIn(
            "nettoyage Docker est incomplet",
            runtime.block_execution.call_args_list[-1].args[0],
        )

    async def test_stop_launched_forgets_handle_only_after_confirmed_cleanup(self):
        task_id = 9002
        handle = object()
        team.launched_apps[task_id] = {"sandbox": handle}
        runtime = SimpleNamespace(stop_handle=AsyncMock())

        with patch.object(team, "get_sandbox_runtime", return_value=runtime):
            await team._stop_launched(task_id)

        self.assertNotIn(task_id, team.launched_apps)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=True)

    async def test_preview_launch_and_stop_share_one_lifecycle_lock(self):
        task_id = 9003
        handle = object()
        started = asyncio.Event()
        release = asyncio.Event()

        async def controlled_launch(_task_id):
            started.set()
            await release.wait()
            team.launched_apps[task_id] = {"sandbox": handle}
            return {"type": "python", "running": True}

        runtime = SimpleNamespace(stop_handle=AsyncMock())
        team._launched_app_locks.pop(task_id, None)
        with (
            patch.object(team, "_launch_app_locked", side_effect=controlled_launch),
            patch.object(team, "get_sandbox_runtime", return_value=runtime),
        ):
            launching = asyncio.create_task(team.launch_app(task_id))
            await started.wait()
            stopping = asyncio.create_task(team.launch_stop(task_id))
            await asyncio.sleep(0)
            self.assertFalse(stopping.done())
            release.set()
            self.assertTrue((await launching)["running"])
            self.assertEqual(await stopping, {"stopped": True})

        self.assertNotIn(task_id, team.launched_apps)
        runtime.stop_handle.assert_awaited_once_with(handle, strict=True)
        team._launched_app_locks.pop(task_id, None)

    async def test_two_preview_launches_never_overlap(self):
        task_id = 9004
        active = 0
        maximum = 0

        async def controlled_launch(_task_id):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {"type": "python", "running": True}

        team._launched_app_locks.pop(task_id, None)
        with patch.object(team, "_launch_app_locked", side_effect=controlled_launch):
            await asyncio.gather(team.launch_app(task_id), team.launch_app(task_id))

        self.assertEqual(maximum, 1)
        team._launched_app_locks.pop(task_id, None)

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

    async def test_delete_keeps_terminal_result_until_cleanup(self):
        task_id = await self._task("delete-completed")
        await team.db_update_task(task_id, status="done")
        observed = []

        async def cleanup(cleanup_task_id, snapshot):
            observed.append((cleanup_task_id, snapshot["status"]))

        with patch.object(team, "_cleanup_task_runtime", side_effect=cleanup):
            self.assertEqual(await team.delete_task(task_id), {"deleted": True})

        self.assertEqual(observed, [(task_id, "done")])
        self.assertIsNone(await team.db_get_task(task_id))

    async def test_pause_wins_before_spawn_without_being_overwritten(self):
        task_id = await self._task("pause-before-spawn")
        job = SimpleNamespace(
            id="race-job",
            kind="run_task",
            idempotency_key=f"task:{task_id}:run:0",
            payload={"task_id": task_id, "resume": False},
        )
        await team.db_update_task(
            task_id, status="queued", queue_job_id=job.id
        )
        real_claim = team.db_claim_task_for_job

        async def pause_then_claim(claimed_task_id, job_id):
            await team.db_transition_task_status(
                claimed_task_id, "paused", {"queued"}
            )
            return await real_claim(claimed_task_id, job_id)

        with patch.object(
            team, "db_claim_task_for_job", side_effect=pause_then_claim
        ), patch.object(team, "_spawn_loop") as spawn:
            result = await team._execute_run_job(job)

        spawn.assert_not_called()
        self.assertEqual(result["status"], "paused")
        self.assertEqual((await team.db_get_task(task_id))["status"], "paused")

    async def test_notification_retry_keeps_context_without_restarting_agent(self):
        task_id = await self._task("resume-context")
        job = SimpleNamespace(
            id="resume-job",
            kind="run_task",
            idempotency_key=f"task:{task_id}:run:0",
            payload={
                "task_id": task_id,
                "resume": False,
                "notification_channel": "alerts",
            },
        )
        await team.db_update_task(
            task_id,
            status="queued",
            queue_job_id=job.id,
            iteration=3,
            total_cost_usd=2.5,
        )
        observed = {}

        def fake_spawn(spawned_task_id, resume=False):
            observed["task_id"] = spawned_task_id
            observed["resume"] = resume

            async def finish():
                await team.db_transition_task_status(
                    spawned_task_id, "done", {"running"}
                )

            return asyncio.create_task(finish())

        with patch.object(team, "_spawn_loop", side_effect=fake_spawn) as spawn, patch.object(
            team,
            "enqueue_notification",
            new=AsyncMock(side_effect=[RuntimeError("queue unavailable"), "notify-job"]),
        ) as notify:
            result = await team._execute_run_job(job)
            dispatched = await team._flush_completion_notification_outbox()

        self.assertEqual(observed, {"task_id": task_id, "resume": True})
        self.assertEqual(result["status"], "done")
        self.assertIsNone(result["notification_job_id"])
        self.assertEqual(dispatched, 1)
        self.assertEqual(spawn.call_count, 1)
        self.assertEqual(notify.await_count, 2)
        async with team.aiosqlite.connect(team.DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM completion_notification_outbox"
            ) as cursor:
                self.assertEqual((await cursor.fetchone())[0], 0)
        persisted = await team.db_get_task(task_id)
        self.assertEqual(persisted["iteration"], 3)
        self.assertEqual(persisted["total_cost_usd"], 2.5)

    async def test_running_task_with_matching_recovered_job_is_restarted(self):
        task_id = await self._task("crash-recovery")
        job = await team.enqueue_task_run(task_id, resume=False)
        await team.db_update_task(
            task_id,
            status="running",
            iteration=2,
            total_cost_usd=1.25,
        )
        observed = {}

        def fake_spawn(spawned_task_id, resume=False):
            observed.update(task_id=spawned_task_id, resume=resume)

            async def finish():
                await team.db_transition_task_status(
                    spawned_task_id, "done", {"running"}
                )

            return asyncio.create_task(finish())

        with patch.object(team, "_spawn_loop", side_effect=fake_spawn):
            result = await team._execute_run_job(job)

        self.assertEqual(observed, {"task_id": task_id, "resume": True})
        self.assertEqual(result["status"], "done")

    async def test_task_run_reservation_is_atomic_and_idempotent(self):
        task_id = await self._task("atomic-reservation")
        first, second = await asyncio.gather(
            team.enqueue_task_run(task_id, resume=False),
            team.enqueue_task_run(task_id, resume=False),
        )
        self.assertEqual(first.id, second.id)
        task = await team.db_get_task(task_id)
        self.assertEqual(task["status"], "queued")
        self.assertEqual(task["queue_job_id"], first.id)
        self.assertEqual(task["run_number"], 1)
        jobs = await team.task_queue.list()
        self.assertEqual([job.id for job in jobs], [first.id])

    async def test_failed_reservation_rolls_back_the_job_insert(self):
        task_id = await self._task("reservation-rollback")
        with self.assertRaises(team.TaskReservationConflict):
            await team.enqueue_task_run(
                task_id,
                resume=True,
                expected_statuses={"paused"},
            )
        self.assertEqual(await team.task_queue.list(), [])
        task = await team.db_get_task(task_id)
        self.assertEqual(task["status"], "idle")
        self.assertIsNone(task["queue_job_id"])

    async def test_reconcile_requeues_running_task_whose_job_is_terminal(self):
        task_id = await self._task("reconcile-running")
        old_job = await team.enqueue_task_run(task_id, resume=False)
        old_lease = await team.task_queue.claim(worker_id="old-worker")
        self.assertEqual(old_lease.job.id, old_job.id)
        await team.task_queue.complete(old_lease)
        await team.db_update_task(task_id, status="running")

        await team._reconcile_durable_tasks()

        task = await team.db_get_task(task_id)
        self.assertEqual(task["status"], "queued")
        self.assertNotEqual(task["queue_job_id"], old_job.id)
        self.assertEqual(task["run_number"], 2)
        self.assertEqual(
            (await team.task_queue.get(task["queue_job_id"])).state,
            JobState.QUEUED,
        )

    async def test_concurrent_resume_loser_never_cancels_winner_job(self):
        task_id = await self._task("concurrent-resume")
        job = await team.enqueue_task_run(task_id, resume=False)
        await team.task_queue.pause(job.id)
        await team.db_transition_task_status(task_id, "paused", {"queued"})

        results = await asyncio.gather(
            team.resume_task(task_id),
            team.resume_task(task_id),
            return_exceptions=True,
        )

        self.assertEqual(sum(isinstance(item, HTTPException) for item in results), 1)
        self.assertEqual(sum(isinstance(item, dict) for item in results), 1)
        self.assertEqual((await team.task_queue.get(job.id)).state, JobState.QUEUED)
        self.assertEqual((await team.db_get_task(task_id))["status"], "queued")

    async def test_worker_failure_cas_preserves_human_and_terminal_states(self):
        task_id = await self._task("failure-cas")
        await team.db_update_task(
            task_id, status="paused", queue_job_id="owned-job"
        )
        changed = await team.db_transition_task_status_for_job(
            task_id,
            "owned-job",
            "queued",
            {"running", "queued", "failed"},
        )
        self.assertFalse(changed)
        self.assertEqual((await team.db_get_task(task_id))["status"], "paused")

        await team.db_update_task(task_id, status="done")
        with self.assertRaises(HTTPException) as stopped:
            await team.stop_task(task_id)
        self.assertEqual(stopped.exception.status_code, 409)
        self.assertEqual((await team.db_get_task(task_id))["status"], "done")

    async def test_stale_pause_snapshot_never_restores_over_terminal_state(self):
        task_id = await self._task("already-finished")
        await team.db_update_task(
            task_id, status="done", queue_job_id="finished-job"
        )
        stale = await team.db_get_task(task_id)
        stale["status"] = "running"

        with patch.object(team, "db_get_task", new=AsyncMock(return_value=stale)):
            with self.assertRaises(HTTPException) as raised:
                await team.pause_task(task_id)

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual((await team.db_get_task(task_id))["status"], "done")

    async def test_lease_loss_cancels_the_nested_agent_work(self):
        job = await team.task_queue.enqueue(
            kind="run_task",
            payload={"task_id": 1},
            idempotency_key="lease:cancellation",
        )
        lease = await team.task_queue.claim(
            worker_id="worker:lease-test", lease_seconds=60
        )
        self.assertIsNotNone(lease)
        cancelled = asyncio.Event()

        async def long_work(_job):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        with patch.object(
            team, "_execute_queue_job", side_effect=long_work
        ), patch.object(
            team.task_queue,
            "heartbeat",
            new=AsyncMock(side_effect=LeaseLost("lost")),
        ), patch.object(team, "_QUEUE_LEASE_SECONDS", 0.03):
            with self.assertRaises(LeaseLost):
                await team._run_with_heartbeat(lease)

        self.assertTrue(cancelled.is_set())
        self.assertEqual(job.id, lease.job.id)

    async def test_terminal_intent_is_recovered_after_last_attempt_crash(self):
        task_id = await self._task("last-attempt-notification")
        with patch.object(
            team, "_configured_channels", new=AsyncMock(return_value=("alerts",))
        ):
            job = await team.enqueue_task_run(
                task_id, resume=False, notification_channel="alerts"
            )

        def crash_after_done(spawned_task_id, resume=False):
            async def crash():
                await team.db_transition_task_status(
                    spawned_task_id, "done", {"running"}
                )
                raise asyncio.CancelledError

            return asyncio.create_task(crash())

        with patch.object(team, "_spawn_loop", side_effect=crash_after_done):
            with self.assertRaises(asyncio.CancelledError):
                await team._execute_run_job(job)

        db = sqlite3.connect(team.DB_PATH)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT ready FROM completion_notification_outbox "
                    "WHERE run_job_id=?",
                    (job.id,),
                ).fetchone()[0],
                0,
            )
        finally:
            db.close()

        with patch.object(
            team, "enqueue_notification", new=AsyncMock(return_value="too-early")
        ) as premature:
            self.assertEqual(
                await team._flush_completion_notification_outbox(), 0
            )
        premature.assert_not_awaited()

        db = sqlite3.connect(team.DB_PATH)
        try:
            db.execute(
                "UPDATE durable_jobs SET state='failed', attempts=max_attempts, "
                "finished_at=? WHERE id=?",
                (T0.isoformat(), job.id),
            )
            db.commit()
        finally:
            db.close()

        with patch.object(
            team, "enqueue_notification", new=AsyncMock(return_value="notify-recovered")
        ) as notify:
            self.assertEqual(
                await team._flush_completion_notification_outbox(), 1
            )
        notify.assert_awaited_once()
        db = sqlite3.connect(team.DB_PATH)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM completion_notification_outbox"
                ).fetchone()[0],
                0,
            )
        finally:
            db.close()

    async def test_wrong_active_job_is_rejected_and_reconcile_repairs_reference(self):
        task_id = await self._task("wrong-active-job")
        wrong = await team.task_queue.enqueue(
            kind="notify",
            payload={"task_id": 999, "channel": "alerts", "text": "wrong"},
            idempotency_key="wrong-active-job",
        )
        await team.db_update_task(
            task_id, status="queued", queue_job_id=wrong.id
        )

        with self.assertRaises(team.TaskReservationConflict):
            await team.enqueue_task_run(
                task_id, resume=False, expected_statuses={"queued"}
            )

        await team._reconcile_durable_tasks()
        repaired = await team.db_get_task(task_id)
        self.assertNotEqual(repaired["queue_job_id"], wrong.id)
        self.assertEqual(repaired["run_number"], 1)
        replacement = await team.task_queue.get(repaired["queue_job_id"])
        self.assertEqual(replacement.kind, "run_task")
        self.assertEqual(replacement.payload["task_id"], task_id)
        self.assertEqual((await team.task_queue.get(wrong.id)).state, JobState.QUEUED)

    async def test_pause_and_resume_are_ordered_before_concurrent_stop(self):
        pause_id = await self._task("pause-stop-order")
        pause_job = await team.enqueue_task_run(pause_id, resume=False)
        pause_entered = asyncio.Event()
        pause_release = asyncio.Event()
        events = []
        original_pause = team.task_queue.pause

        async def controlled_pause(job_id, *args, **kwargs):
            result = await original_pause(job_id, *args, **kwargs)
            pause_entered.set()
            await pause_release.wait()
            return result

        async def record_emit(_task_id, _iteration, _agent, kind, _data):
            events.append(kind)

        async def no_cleanup(*_args, **_kwargs):
            return None

        with patch.object(
            team.task_queue, "pause", side_effect=controlled_pause
        ), patch.object(team, "emit", side_effect=record_emit), patch.object(
            team, "_cleanup_task_runtime", side_effect=no_cleanup
        ):
            pausing = asyncio.create_task(team.pause_task(pause_id))
            await pause_entered.wait()
            stopping = asyncio.create_task(team.stop_task(pause_id))
            await asyncio.sleep(0)
            self.assertFalse(stopping.done())
            pause_release.set()
            self.assertEqual(await pausing, {"status": "paused"})
            self.assertEqual(await stopping, {"status": "stopped"})

        self.assertEqual(events, ["paused", "stopped"])
        self.assertEqual((await team.db_get_task(pause_id))["status"], "stopped")
        self.assertEqual(
            (await team.task_queue.get(pause_job.id)).state, JobState.CANCELLED
        )

        resume_id = await self._task("resume-stop-order")
        resume_job = await team.enqueue_task_run(resume_id, resume=False)
        await team.task_queue.pause(resume_job.id)
        await team.db_transition_task_status(resume_id, "paused", {"queued"})
        resume_entered = asyncio.Event()
        resume_release = asyncio.Event()
        events.clear()
        original_resume = team.task_queue.resume

        async def controlled_resume(job_id, *args, **kwargs):
            result = await original_resume(job_id, *args, **kwargs)
            resume_entered.set()
            await resume_release.wait()
            return result

        with patch.object(
            team.task_queue, "resume", side_effect=controlled_resume
        ), patch.object(team, "emit", side_effect=record_emit), patch.object(
            team, "_cleanup_task_runtime", side_effect=no_cleanup
        ):
            resuming = asyncio.create_task(team.resume_task(resume_id))
            await resume_entered.wait()
            stopping = asyncio.create_task(team.stop_task(resume_id))
            await asyncio.sleep(0)
            self.assertFalse(stopping.done())
            resume_release.set()
            self.assertEqual((await resuming)["status"], "queued")
            self.assertEqual(await stopping, {"status": "stopped"})

        self.assertEqual(events, ["resumed", "stopped"])
        self.assertEqual((await team.db_get_task(resume_id))["status"], "stopped")

    async def test_stop_discards_pending_intent_and_worker_cannot_recreate_it(self):
        task_id = await self._task("stop-pending-intent")
        with patch.object(
            team, "_configured_channels", new=AsyncMock(return_value=("alerts",))
        ):
            job = await team.enqueue_task_run(
                task_id, resume=False, notification_channel="alerts"
            )
        await team._ensure_completion_notification_intent(
            run_job_id=job.id, task_id=task_id, channel="alerts"
        )

        with patch.object(
            team, "_cleanup_task_runtime", new=AsyncMock()
        ):
            self.assertEqual(await team.stop_task(task_id), {"status": "stopped"})
        self.assertEqual((await team._execute_run_job(job))["status"], "stopped")
        db = sqlite3.connect(team.DB_PATH)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM completion_notification_outbox "
                    "WHERE task_id=?",
                    (task_id,),
                ).fetchone()[0],
                0,
            )
        finally:
            db.close()

    async def test_claim_commit_and_spawn_are_atomic_against_stop(self):
        task_id = await self._task("claim-spawn-stop")
        job = await team.enqueue_task_run(task_id, resume=False)
        claim_committed = asyncio.Event()
        claim_release = asyncio.Event()
        real_claim = team.db_claim_task_for_job

        async def controlled_claim(claimed_task_id, job_id):
            result = await real_claim(claimed_task_id, job_id)
            claim_committed.set()
            await claim_release.wait()
            return result

        async def blocked_agent(*_args, **_kwargs):
            await asyncio.Future()

        with patch.object(
            team, "db_claim_task_for_job", side_effect=controlled_claim
        ), patch.object(team, "_run_task", side_effect=blocked_agent), patch.object(
            team, "_cleanup_task_runtime", new=AsyncMock()
        ):
            executing = asyncio.create_task(team._execute_run_job(job))
            await claim_committed.wait()
            stopping = asyncio.create_task(team.stop_task(task_id))
            await asyncio.sleep(0)
            self.assertFalse(stopping.done())
            self.assertEqual((await team.db_get_task(task_id))["status"], "running")
            claim_release.set()
            self.assertEqual(await stopping, {"status": "stopped"})
            result = await asyncio.gather(executing, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual((await team.db_get_task(task_id))["status"], "stopped")

    async def test_delete_wins_before_worker_failure_notification_path(self):
        task_id = await self._task("delete-worker-failure")
        with patch.object(
            team, "_configured_channels", new=AsyncMock(return_value=("alerts",))
        ):
            job = await team.enqueue_task_run(
                task_id, resume=False, notification_channel="alerts"
            )
        db = sqlite3.connect(team.DB_PATH)
        try:
            db.execute(
                "UPDATE durable_jobs SET max_attempts=1 WHERE id=?", (job.id,)
            )
            db.commit()
        finally:
            db.close()

        failed = asyncio.Event()
        failure_release = asyncio.Event()
        real_fail = team.task_queue.fail
        previous_shutdown = team._runtime_shutdown
        team._runtime_shutdown = asyncio.Event()

        async def controlled_fail(*args, **kwargs):
            result = await real_fail(*args, **kwargs)
            failed.set()
            await failure_release.wait()
            return result

        async def explode(_job):
            raise RuntimeError("agent failed")

        try:
            with patch.object(
                team.task_queue, "fail", side_effect=controlled_fail
            ), patch.object(
                team, "_execute_queue_job", side_effect=explode
            ), patch.object(
                team, "_cleanup_task_runtime", new=AsyncMock()
            ), patch.object(
                team, "_stage_completion_notification", wraps=team._stage_completion_notification
            ) as stage:
                worker = asyncio.create_task(team._queue_worker(77))
                await failed.wait()
                self.assertEqual(await team.delete_task(task_id), {"deleted": True})
                failure_release.set()
                team._runtime_shutdown.set()
                await asyncio.wait_for(worker, timeout=1)
            stage.assert_not_awaited()
        finally:
            team._runtime_shutdown = previous_shutdown

        self.assertIsNone(await team.db_get_task(task_id))
        db = sqlite3.connect(team.DB_PATH)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM completion_notification_outbox "
                    "WHERE task_id=?",
                    (task_id,),
                ).fetchone()[0],
                0,
            )
        finally:
            db.close()

    async def test_delete_wins_before_completion_dispatch(self):
        task_id = await self._task("delete-finalization")
        with patch.object(
            team, "_configured_channels", new=AsyncMock(return_value=("alerts",))
        ):
            job = await team.enqueue_task_run(
                task_id, resume=False, notification_channel="alerts"
            )
        await team.db_update_task(task_id, status="done")
        stage_entered = asyncio.Event()
        stage_release = asyncio.Event()
        real_stage = team._stage_completion_notification

        async def controlled_stage(**kwargs):
            stage_entered.set()
            await stage_release.wait()
            return await real_stage(**kwargs)

        with patch.object(
            team, "_stage_completion_notification", side_effect=controlled_stage
        ), patch.object(team, "_cleanup_task_runtime", new=AsyncMock()):
            finishing = asyncio.create_task(team._execute_run_job(job))
            await stage_entered.wait()
            deleting = asyncio.create_task(team.delete_task(task_id))
            await asyncio.sleep(0)
            self.assertFalse(deleting.done())
            stage_release.set()
            self.assertEqual(await deleting, {"deleted": True})
            result = await finishing

        self.assertIsNone(result["notification_job_id"])
        self.assertIsNone(await team.db_get_task(task_id))
        jobs = await team.task_queue.list(limit=100)
        self.assertFalse(
            any(
                item.kind == "notify"
                and isinstance(item.payload, dict)
                and item.payload.get("task_id") == task_id
                for item in jobs
            )
        )

    async def test_delete_cancels_already_enqueued_notification(self):
        task_id = await self._task("delete-notification")
        await team.db_update_task(task_id, status="done")
        notification = await team.task_queue.enqueue(
            kind="notify",
            payload={"task_id": task_id, "channel": "alerts", "text": "done"},
            idempotency_key="notify-before-delete",
        )
        lease = await team.task_queue.claim(worker_id="notify-worker")
        self.assertEqual(lease.job.id, notification.id)

        with patch.object(team, "_cleanup_task_runtime", new=AsyncMock()):
            self.assertEqual(await team.delete_task(task_id), {"deleted": True})
        self.assertEqual(
            (await team.task_queue.get(notification.id)).state, JobState.CANCELLED
        )
        gateway = SimpleNamespace(send=AsyncMock())
        with patch.object(team, "messaging_gateway", gateway):
            result = await team._execute_queue_job(lease.job)
        self.assertEqual(result["status"], "deleted")
        gateway.send.assert_not_awaited()

    async def test_delete_blocks_new_preview_until_task_is_gone(self):
        task_id = await self._task("delete-preview-race")
        await team.db_update_task(task_id, status="done")
        cleanup_entered = asyncio.Event()
        cleanup_release = asyncio.Event()

        async def controlled_cleanup(*_args, **_kwargs):
            cleanup_entered.set()
            await cleanup_release.wait()

        with patch.object(
            team, "_cleanup_task_runtime", side_effect=controlled_cleanup
        ):
            deleting = asyncio.create_task(team.delete_task(task_id))
            await cleanup_entered.wait()
            launching = asyncio.create_task(team.launch_app(task_id))
            await asyncio.sleep(0)
            self.assertFalse(launching.done())
            cleanup_release.set()
            self.assertEqual(await deleting, {"deleted": True})
            with self.assertRaises(HTTPException) as missing:
                await launching
        self.assertEqual(missing.exception.status_code, 404)

    async def test_failed_shutdown_rollback_exposes_unavailable_runtime(self):
        task_id = await self._task("rollback-failed")
        previous_runtime = team.sandbox_runtime
        previous_shutdown = team._runtime_shutdown
        previous_background = team._runtime_background_tasks
        failed = SandboxStopResult("*", (), (("container", "rm refused"),))
        runtime = SimpleNamespace(
            stop_all_managed=AsyncMock(return_value=failed),
            block_execution=Mock(),
        )
        team.sandbox_runtime = runtime
        team._runtime_shutdown = asyncio.Event()
        team._runtime_background_tasks = []
        try:
            with patch.object(
                team,
                "start_runtime_services",
                new=AsyncMock(side_effect=RuntimeError("db unavailable")),
            ), patch.object(automation_api, "disable") as disable:
                with self.assertRaises(SandboxStopError):
                    await team.stop_runtime_services()
            self.assertIsNotNone(team._runtime_service_error)
            self.assertEqual(disable.call_count, 2)
            self.assertIn(
                "n'ont pas pu redemarrer", disable.call_args_list[-1].args[0]
            )
            with self.assertRaises(HTTPException) as unavailable:
                await team.start_task(task_id)
            self.assertEqual(unavailable.exception.status_code, 503)
            self.assertEqual(team._runtime_background_tasks, [])
        finally:
            team.sandbox_runtime = previous_runtime
            team._runtime_shutdown = previous_shutdown
            team._runtime_background_tasks = previous_background

    async def test_resistant_local_preview_refuses_global_shutdown(self):
        previous_runtime = team.sandbox_runtime
        previous_shutdown = team._runtime_shutdown
        previous_background = team._runtime_background_tasks
        task_id = 9911

        async def never_exits():
            await asyncio.Future()

        proc = SimpleNamespace(
            returncode=None,
            terminate=Mock(),
            kill=Mock(),
            wait=Mock(side_effect=never_exits),
        )
        runtime = SimpleNamespace(
            stop_all_managed=AsyncMock(
                return_value=SandboxStopResult("*", (), ())
            ),
            block_execution=Mock(),
        )
        team.sandbox_runtime = runtime
        team._runtime_shutdown = asyncio.Event()
        team._runtime_background_tasks = []
        team.launched_apps[task_id] = {"proc": proc}
        try:
            with patch.object(team, "_PREVIEW_STOP_TIMEOUT_SECONDS", 0.01), patch.object(
                team, "start_runtime_services", new=AsyncMock()
            ) as restart:
                with self.assertRaises(team.PreviewStopError):
                    await team.stop_runtime_services()
                restart.assert_awaited_once()
            self.assertIn(task_id, team.launched_apps)
            proc.terminate.assert_called_once()
            proc.kill.assert_called_once()
            runtime.stop_all_managed.assert_awaited_once_with(strict=False)
        finally:
            team.launched_apps.pop(task_id, None)
            team.sandbox_runtime = previous_runtime
            team._runtime_shutdown = previous_shutdown
            team._runtime_background_tasks = previous_background

    async def test_global_shutdown_waits_for_inflight_preview_then_stops_it(self):
        task_id = await self._task("shutdown-preview-race")
        previous_runtime = team.sandbox_runtime
        previous_shutdown = team._runtime_shutdown
        previous_background = team._runtime_background_tasks
        handle = object()
        launch_entered = asyncio.Event()
        launch_release = asyncio.Event()

        async def controlled_launch(_task_id):
            launch_entered.set()
            await launch_release.wait()
            team.launched_apps[task_id] = {"sandbox": handle}
            return {"running": True}

        runtime = SimpleNamespace(
            stop_handle=AsyncMock(),
            stop_all_managed=AsyncMock(
                return_value=SandboxStopResult("*", (), ())
            ),
            block_execution=Mock(),
        )
        team.sandbox_runtime = runtime
        team._runtime_shutdown = asyncio.Event()
        team._runtime_background_tasks = []
        try:
            with patch.object(
                team, "_launch_app_locked", side_effect=controlled_launch
            ):
                launching = asyncio.create_task(team.launch_app(task_id))
                await launch_entered.wait()
                stopping = asyncio.create_task(team.stop_runtime_services())
                await asyncio.sleep(0)
                self.assertFalse(stopping.done())
                launch_release.set()
                self.assertEqual(await launching, {"running": True})
                await stopping
            runtime.stop_handle.assert_awaited_once_with(handle, strict=True)
            self.assertNotIn(task_id, team.launched_apps)
        finally:
            team.launched_apps.pop(task_id, None)
            team.sandbox_runtime = previous_runtime
            team._runtime_shutdown = previous_shutdown
            team._runtime_background_tasks = previous_background

    async def test_host_mcp_servers_are_refused_in_docker_mode(self):
        task_id = await self._task("docker-mcp", mode="docker")
        await team.db_update_task(task_id, status="running")
        folder = str(self.projects / "docker-mcp")

        with patch.object(
            team, "_tool_add_mcp", new=AsyncMock()
        ) as add_mcp, patch.object(
            team, "_tool_mcp_call", new=AsyncMock()
        ) as call_mcp:
            added = await team.execute_tool(
                task_id,
                folder,
                False,
                {"tool": "add_mcp", "server": "filesystem"},
            )
            called = await team.execute_tool(
                task_id,
                folder,
                False,
                {"tool": "mcp_call", "server": "filesystem", "name": "read"},
            )

        self.assertIn("Docker", added)
        self.assertIn("Docker", called)
        add_mcp.assert_not_awaited()
        call_mcp.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
