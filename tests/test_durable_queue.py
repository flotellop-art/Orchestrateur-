import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from durable_queue import (
    DurableQueue,
    IdempotencyConflict,
    JobState,
    LeaseLost,
    ValidationError,
)


T0 = datetime(2026, 7, 11, 8, 0, tzinfo=timezone.utc)


class DurableQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.queue = DurableQueue(Path(self.tempdir.name) / "queue.db")
        await self.queue.init()

    async def asyncTearDown(self):
        self.tempdir.cleanup()

    async def enqueue(self, key="job:1", **extra):
        values = {
            "kind": "run_task",
            "payload": {"objective": "Verifier le projet"},
            "idempotency_key": key,
            "now": T0,
        }
        values.update(extra)
        return await self.queue.enqueue(**values)

    async def test_enqueue_is_idempotent_and_conflicts_fail_closed(self):
        first = await self.enqueue()
        second = await self.enqueue()
        self.assertEqual(first.id, second.id)
        self.assertEqual(first.state, JobState.QUEUED)

        with self.assertRaises(IdempotencyConflict):
            await self.enqueue(payload={"objective": "Autre travail"})

    async def test_claim_is_atomic_and_completion_requires_the_lease(self):
        job = await self.enqueue()

        async def claim(worker):
            return await self.queue.claim(worker_id=worker, now=T0, lease_seconds=30)

        claims = await asyncio.gather(claim("worker:a"), claim("worker:b"))
        leases = [item for item in claims if item is not None]
        self.assertEqual(len(leases), 1)
        lease = leases[0]
        self.assertEqual(lease.job.id, job.id)
        self.assertEqual(lease.job.attempts, 1)

        completed = await self.queue.complete(
            lease, result={"task_id": 42}, now=T0 + timedelta(seconds=1)
        )
        self.assertEqual(completed.state, JobState.SUCCEEDED)
        self.assertEqual(completed.result, {"task_id": 42})
        with self.assertRaises(LeaseLost):
            await self.queue.complete(lease, result={"again": True}, now=T0)

    async def test_expired_lease_is_requeued_after_crash(self):
        job = await self.enqueue(base_backoff_seconds=2)
        lease = await self.queue.claim(worker_id="worker:old", now=T0, lease_seconds=5)
        self.assertIsNotNone(lease)

        recovered = await self.queue.recover_expired_leases(
            now=T0 + timedelta(seconds=6)
        )
        self.assertEqual(recovered, 1)
        queued = await self.queue.get(job.id)
        self.assertEqual(queued.state, JobState.QUEUED)
        self.assertEqual(queued.last_error_code, "lease_expired")
        self.assertIsNone(
            await self.queue.claim(
                worker_id="worker:new", now=T0 + timedelta(seconds=7), lease_seconds=5
            )
        )
        replacement = await self.queue.claim(
            worker_id="worker:new", now=T0 + timedelta(seconds=8), lease_seconds=5
        )
        self.assertIsNotNone(replacement)
        self.assertEqual(replacement.job.attempts, 2)
        with self.assertRaises(LeaseLost):
            await self.queue.complete(lease, now=T0 + timedelta(seconds=8))

    async def test_failures_use_exponential_backoff_and_stop_after_limit(self):
        await self.enqueue(max_attempts=2, base_backoff_seconds=10)
        first = await self.queue.claim(worker_id="worker:retry", now=T0)
        retried = await self.queue.fail(first, error_code="provider_busy", now=T0)
        self.assertEqual(retried.state, JobState.QUEUED)
        self.assertEqual(retried.available_at, T0 + timedelta(seconds=10))
        self.assertIsNone(
            await self.queue.claim(worker_id="worker:retry", now=T0 + timedelta(seconds=9))
        )

        second = await self.queue.claim(
            worker_id="worker:retry", now=T0 + timedelta(seconds=10)
        )
        final = await self.queue.fail(
            second, error_code="provider_busy", now=T0 + timedelta(seconds=10)
        )
        self.assertEqual(final.state, JobState.FAILED)
        self.assertEqual(final.attempts, 2)

    async def test_pause_resume_and_cancel_invalidate_running_lease(self):
        job = await self.enqueue()
        paused = await self.queue.pause(job.id, now=T0)
        self.assertEqual(paused.state, JobState.PAUSED)
        self.assertIsNone(await self.queue.claim(worker_id="worker:1", now=T0))

        resumed = await self.queue.resume(job.id, now=T0)
        self.assertEqual(resumed.state, JobState.QUEUED)
        first_lease = await self.queue.claim(worker_id="worker:1", now=T0)
        paused_running = await self.queue.pause(job.id, now=T0)
        self.assertEqual(paused_running.state, JobState.PAUSED)
        self.assertFalse(await self.queue.lease_is_active(first_lease, now=T0))
        with self.assertRaises(LeaseLost):
            await self.queue.complete(first_lease, now=T0)

        await self.queue.resume(job.id, now=T0)
        second_lease = await self.queue.claim(worker_id="worker:2", now=T0)
        # Une pause volontaire ou une fermeture propre ne doit pas consommer
        # le budget de reprises reserve aux vrais echecs.
        self.assertEqual(second_lease.job.attempts, 1)
        cancelled = await self.queue.cancel(job.id, now=T0)
        self.assertEqual(cancelled.state, JobState.CANCELLED)
        with self.assertRaises(LeaseLost):
            await self.queue.complete(second_lease, now=T0)

    async def test_heartbeat_extends_only_a_live_lease(self):
        await self.enqueue()
        lease = await self.queue.claim(worker_id="worker:heartbeat", now=T0, lease_seconds=5)
        renewed = await self.queue.heartbeat(
            lease, now=T0 + timedelta(seconds=4), lease_seconds=10
        )
        self.assertEqual(renewed.lease_until, T0 + timedelta(seconds=14))
        self.assertTrue(
            await self.queue.lease_is_active(renewed, now=T0 + timedelta(seconds=13))
        )
        with self.assertRaises(LeaseLost):
            await self.queue.heartbeat(
                renewed, now=T0 + timedelta(seconds=15), lease_seconds=10
            )

    async def test_clean_shutdown_releases_lease_atomically_and_refunds_attempt(self):
        job = await self.enqueue()
        lease = await self.queue.claim(
            worker_id="worker:shutdown", now=T0, lease_seconds=30
        )
        released = await self.queue.release(lease, now=T0 + timedelta(seconds=1))
        self.assertEqual(released.state, JobState.QUEUED)
        self.assertEqual(released.attempts, 0)
        self.assertIsNone(released.lease_owner)
        with self.assertRaises(LeaseLost):
            await self.queue.complete(lease, now=T0 + timedelta(seconds=2))
        replacement = await self.queue.claim(
            worker_id="worker:restart", now=T0 + timedelta(seconds=2)
        )
        self.assertEqual(replacement.job.id, job.id)
        self.assertEqual(replacement.job.attempts, 1)

    async def test_deleting_template_cancels_only_its_scheduled_occurrences(self):
        payload_a = {
            "automation_id": "a" * 32,
            "scheduled_for": "2026-07-11T08:00:00Z",
            "payload": {"task_id": 7},
        }
        payload_b = {
            "automation_id": "b" * 32,
            "scheduled_for": "2026-07-11T08:00:00Z",
            "payload": {"task_id": 8},
        }
        first = await self.enqueue(key="scheduled:7", payload=payload_a)
        second = await self.enqueue(key="scheduled:8", payload=payload_b)
        self.assertEqual(await self.queue.cancel_scheduled_for_task(7, now=T0), 1)
        self.assertEqual((await self.queue.get(first.id)).state, JobState.CANCELLED)
        self.assertEqual((await self.queue.get(second.id)).state, JobState.QUEUED)

    async def test_corrupt_scheduled_payload_is_quarantined_without_blocking_delete(self):
        target = await self.enqueue(
            key="scheduled:target",
            payload={
                "automation_id": "a" * 32,
                "scheduled_for": "2026-07-11T08:00:00Z",
                "payload": {"task_id": 7},
            },
        )
        corrupt = await self.enqueue(
            key="scheduled:corrupt",
            payload={
                "automation_id": "b" * 32,
                "scheduled_for": "2026-07-11T08:00:00Z",
                "payload": {"task_id": 8},
            },
        )
        db = sqlite3.connect(self.queue.db_path)
        try:
            db.execute(
                "UPDATE durable_jobs SET payload_json='{broken' WHERE id=?",
                (corrupt.id,),
            )
            db.commit()
        finally:
            db.close()

        self.assertEqual(
            await self.queue.cancel_scheduled_for_task(7, now=T0), 1
        )
        self.assertEqual((await self.queue.get(target.id)).state, JobState.CANCELLED)
        db = sqlite3.connect(self.queue.db_path)
        try:
            corrupt_state = db.execute(
                "SELECT state FROM durable_jobs WHERE id=?", (corrupt.id,)
            ).fetchone()[0]
        finally:
            db.close()
        self.assertEqual(corrupt_state, JobState.CANCELLED.value)

    async def test_notification_cancellation_matches_strict_integer_task_id(self):
        exact = await self.queue.enqueue(
            kind="notify",
            payload={"task_id": 1, "channel": "alerts", "text": "exact"},
            idempotency_key="notify:exact",
            now=T0,
        )
        boolean = await self.queue.enqueue(
            kind="notify",
            payload={"task_id": True, "channel": "alerts", "text": "bool"},
            idempotency_key="notify:boolean",
            now=T0,
        )

        self.assertEqual(
            await self.queue.cancel_notifications_for_task(1, now=T0), 1
        )
        self.assertEqual((await self.queue.get(exact.id)).state, JobState.CANCELLED)
        self.assertEqual((await self.queue.get(boolean.id)).state, JobState.QUEUED)

    async def test_worker_cannot_finish_after_lease_deadline(self):
        await self.enqueue()
        lease = await self.queue.claim(
            worker_id="worker:late", now=T0, lease_seconds=5
        )
        with self.assertRaises(LeaseLost):
            await self.queue.complete(lease, now=T0 + timedelta(seconds=6))
        self.assertEqual(
            await self.queue.recover_expired_leases(now=T0 + timedelta(seconds=6)),
            1,
        )

    async def test_payloads_and_error_codes_are_bounded(self):
        with self.assertRaises(ValidationError):
            await self.enqueue(payload={"text": "x" * (70 * 1024)})
        await self.enqueue()
        lease = await self.queue.claim(worker_id="worker:safe", now=T0)
        with self.assertRaises(ValidationError):
            await self.queue.fail(
                lease,
                error_code="https://secret.example/token/value",
                now=T0,
            )

    async def test_purge_removes_only_terminal_jobs_past_retention(self):
        old_done = await self.enqueue(key="old:done")
        old_lease = await self.queue.claim(worker_id="worker:old-done", now=T0)
        await self.queue.complete(old_lease, now=T0 + timedelta(seconds=30))

        old_waiting = await self.enqueue(key="old:waiting")
        recent_done = await self.enqueue(
            key="recent:done", now=T0 + timedelta(days=40), priority=10
        )
        recent_lease = await self.queue.claim(
            worker_id="worker:recent", now=T0 + timedelta(days=40)
        )
        await self.queue.complete(
            recent_lease, now=T0 + timedelta(days=40, seconds=30)
        )

        removed = await self.queue.purge_terminal(
            older_than=T0 + timedelta(days=30)
        )
        self.assertEqual(removed, 1)
        self.assertIsNone(await self.queue.get(old_done.id))
        self.assertEqual((await self.queue.get(old_waiting.id)).state, JobState.QUEUED)
        self.assertEqual((await self.queue.get(recent_done.id)).state, JobState.SUCCEEDED)

        db = sqlite3.connect(self.queue.db_path)
        try:
            indexes = {
                row[1]: row for row in db.execute("PRAGMA index_list(durable_jobs)")
            }
        finally:
            db.close()
        self.assertIn("idx_durable_jobs_created_at", indexes)


if __name__ == "__main__":
    unittest.main()
