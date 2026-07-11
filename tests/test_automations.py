import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from automations import (
    MAX_AUTOMATION_PAYLOAD_BYTES,
    AutomationConflict,
    AutomationState,
    AutomationStore,
    AutomationValidationError,
    Schedule,
)
from durable_queue import DurableQueue, JobState, datetime_to_text


T0 = datetime(2026, 7, 6, 8, 0, tzinfo=timezone.utc)  # lundi


class ScheduleTests(unittest.TestCase):
    def test_interval_is_anchored_and_strictly_after_reference(self):
        schedule = Schedule.interval(90, anchor=T0)
        self.assertEqual(schedule.next_after(T0 - timedelta(seconds=1)), T0)
        self.assertEqual(schedule.next_after(T0), T0 + timedelta(seconds=90))
        self.assertEqual(
            schedule.next_after(T0 + timedelta(seconds=91)),
            T0 + timedelta(seconds=180),
        )

    def test_daily_is_deterministic_in_utc(self):
        schedule = Schedule.daily(hour=9, minute=30)
        self.assertEqual(
            schedule.next_after(T0),
            datetime(2026, 7, 6, 9, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(
            schedule.next_after(datetime(2026, 7, 6, 9, 30, tzinfo=timezone.utc)),
            datetime(2026, 7, 7, 9, 30, tzinfo=timezone.utc),
        )

    def test_weekly_uses_monday_zero_and_rolls_to_next_week(self):
        friday = Schedule.weekly(weekday=4, hour=17)
        self.assertEqual(
            friday.next_after(T0),
            datetime(2026, 7, 10, 17, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            friday.next_after(datetime(2026, 7, 10, 17, 0, tzinfo=timezone.utc)),
            datetime(2026, 7, 17, 17, 0, tzinfo=timezone.utc),
        )

    def test_payload_round_trip_and_unknown_fields_are_rejected(self):
        schedules = (
            Schedule.interval(60, anchor=T0),
            Schedule.daily(hour=7, minute=15),
            Schedule.weekly(weekday=2, hour=18, minute=5),
        )
        for schedule in schedules:
            with self.subTest(kind=schedule.kind):
                self.assertEqual(Schedule.from_payload(schedule.to_payload()), schedule)
        with self.assertRaises(AutomationValidationError):
            Schedule.from_payload(
                {"type": "daily", "hour": 9, "timezone": "Europe/Paris"}
            )
        with self.assertRaises(AutomationValidationError):
            Schedule.from_payload(
                {"type": "interval", "seconds": 60, "anchor": "2026-07-06T08:00:00"}
            )


class AutomationStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = Path(self.tempdir.name) / "orchestrator.db"
        self.queue = DurableQueue(self.path)
        self.store = AutomationStore(self.path)
        await self.queue.init()
        await self.store.init()

    async def asyncTearDown(self):
        self.tempdir.cleanup()

    async def create(self, **extra):
        values = {
            "name": "Rapport regulier",
            "schedule": Schedule.interval(60, anchor=T0),
            "job_kind": "run_task",
            "payload": {"objective": "Faire le rapport"},
            "idempotency_key": "automation:report",
            "now": T0,
        }
        values.update(extra)
        return await self.store.create(**values)

    async def test_create_is_idempotent(self):
        first = await self.create()
        second = await self.create(now=T0 + timedelta(seconds=30))
        self.assertEqual(first.id, second.id)
        self.assertEqual(first.next_run_at, T0 + timedelta(seconds=60))
        with self.assertRaises(AutomationConflict):
            await self.create(payload={"objective": "Autre travail"})

    async def test_pause_resume_and_cancel(self):
        automation = await self.create()
        paused = await self.store.pause(automation.id, now=T0 + timedelta(seconds=10))
        self.assertEqual(paused.state, AutomationState.PAUSED)
        self.assertEqual(
            await self.store.dispatch_due(self.queue, now=T0 + timedelta(minutes=5)),
            [],
        )

        resumed = await self.store.resume(automation.id, now=T0 + timedelta(minutes=5))
        self.assertEqual(resumed.state, AutomationState.ENABLED)
        self.assertEqual(resumed.next_run_at, T0 + timedelta(minutes=6))
        cancelled = await self.store.cancel(automation.id, now=T0 + timedelta(minutes=5))
        self.assertEqual(cancelled.state, AutomationState.CANCELLED)
        still_cancelled = await self.store.resume(
            automation.id, now=T0 + timedelta(minutes=6)
        )
        self.assertEqual(still_cancelled.state, AutomationState.CANCELLED)

    async def test_cancel_for_task_is_not_limited_to_a_ui_page(self):
        first = await self.create(
            payload={"task_id": 7}, idempotency_key="automation:task7:first"
        )
        second = await self.create(
            payload={"task_id": 8}, idempotency_key="automation:task8"
        )
        third = await self.create(
            payload={"task_id": 7}, idempotency_key="automation:task7:second"
        )
        self.assertEqual(await self.store.cancel_for_task(7, now=T0), 2)
        self.assertEqual((await self.store.get(first.id)).state, AutomationState.CANCELLED)
        self.assertEqual((await self.store.get(third.id)).state, AutomationState.CANCELLED)
        self.assertEqual((await self.store.get(second.id)).state, AutomationState.ENABLED)

    async def test_dispatch_coalesces_missed_occurrences_without_duplicates(self):
        automation = await self.create()
        jobs = await self.store.dispatch_due(
            self.queue, now=T0 + timedelta(minutes=3), limit=10
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(
            [job.payload["scheduled_for"] for job in jobs],
            [datetime_to_text(T0 + timedelta(minutes=1))],
        )
        self.assertTrue(all(job.state is JobState.QUEUED for job in jobs))
        self.assertEqual(
            await self.store.dispatch_due(
                self.queue, now=T0 + timedelta(minutes=3), limit=10
            ),
            [],
        )
        updated = await self.store.get(automation.id)
        self.assertEqual(updated.next_run_at, T0 + timedelta(minutes=4))
        self.assertEqual(len(await self.queue.list()), 1)

    async def test_replay_after_crash_uses_same_queue_job(self):
        automation = await self.create()
        occurrence = T0 + timedelta(minutes=1)
        occurrence_text = datetime_to_text(occurrence)
        payload = {
            "automation_id": automation.id,
            "scheduled_for": occurrence_text,
            "payload": automation.payload,
        }
        existing = await self.queue.enqueue(
            kind=automation.job_kind,
            payload=payload,
            idempotency_key=f"automation:{automation.id}:{occurrence_text}",
            available_at=occurrence,
            now=occurrence,
        )

        dispatched = await self.store.dispatch_due(self.queue, now=occurrence)
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(dispatched[0].id, existing.id)
        self.assertEqual(len(await self.queue.list()), 1)

    async def test_two_schedulers_do_not_duplicate_one_due_occurrence(self):
        await self.create()
        results = await asyncio.gather(
            self.store.dispatch_due(self.queue, now=T0 + timedelta(minutes=1)),
            self.store.dispatch_due(self.queue, now=T0 + timedelta(minutes=1)),
        )
        self.assertEqual(sum(len(items) for items in results), 1)
        self.assertEqual(len(await self.queue.list()), 1)

    async def test_create_validates_the_final_queue_envelope_size(self):
        # Le JSON utilisateur seul tient encore dans 64 Kio, mais les champs
        # automation_id/scheduled_for le feraient depasser dans la file.
        payload = {"text": "x" * (MAX_AUTOMATION_PAYLOAD_BYTES - 20)}
        with self.assertRaises(AutomationValidationError):
            await self.create(
                payload=payload,
                idempotency_key="automation:almost-too-large",
            )

    async def test_invalid_row_is_quarantined_without_blocking_later_due_jobs(self):
        valid = await self.create(idempotency_key="automation:valid-after-bad")
        async with self.store._db() as db:
            await db.execute(
                """
                INSERT INTO automations (
                    id,idempotency_key,name,schedule_json,job_kind,payload_json,
                    state,next_run_at,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,'enabled',?,?,?)
                """,
                (
                    "bad-automation",
                    "automation:bad-row",
                    "invalide",
                    json.dumps(
                        Schedule.interval(60, anchor=T0).to_payload(),
                        separators=(",", ":"),
                    ),
                    "run_task",
                    json.dumps(
                        {"text": "x" * (MAX_AUTOMATION_PAYLOAD_BYTES - 20)},
                        separators=(",", ":"),
                    ),
                    datetime_to_text(T0),
                    datetime_to_text(T0 - timedelta(seconds=1)),
                    datetime_to_text(T0),
                ),
            )
            await db.commit()

        jobs = await self.store.dispatch_due(
            self.queue, now=T0 + timedelta(minutes=1), limit=10
        )
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].payload["automation_id"], valid.id)
        async with self.store._db() as db:
            async with db.execute(
                "SELECT state FROM automations WHERE id='bad-automation'"
            ) as cursor:
                bad = await cursor.fetchone()
        self.assertEqual(bad["state"], AutomationState.CANCELLED.value)

    async def test_legacy_null_id_is_quarantined_by_storage_rowid(self):
        async with self.store._db() as db:
            cursor = await db.execute(
                """
                INSERT INTO automations (
                    id,idempotency_key,name,schedule_json,job_kind,payload_json,
                    state,next_run_at,created_at,updated_at
                ) VALUES (NULL,?,?,?,?,?,'enabled',?,?,?)
                """,
                (
                    "automation:null-id",
                    "ancienne ligne",
                    json.dumps(
                        Schedule.interval(60, anchor=T0).to_payload(),
                        separators=(",", ":"),
                    ),
                    "run_task",
                    json.dumps({"task_id": 1}, separators=(",", ":")),
                    datetime_to_text(T0),
                    datetime_to_text(T0 - timedelta(seconds=1)),
                    datetime_to_text(T0),
                ),
            )
            row_id = cursor.lastrowid
            await db.commit()

        self.assertEqual(
            await self.store.dispatch_due(self.queue, now=T0, limit=10), []
        )
        async with self.store._db() as db:
            async with db.execute(
                "SELECT state FROM automations WHERE rowid=?", (row_id,)
            ) as cursor:
                row = await cursor.fetchone()
        self.assertEqual(row["state"], AutomationState.CANCELLED.value)

    async def test_corrupt_unrelated_payload_does_not_block_task_cancellation(self):
        target = await self.create(
            payload={"task_id": 7},
            idempotency_key="automation:target-task",
        )
        unrelated = await self.create(
            payload={"task_id": 8},
            idempotency_key="automation:other-task",
        )
        generic = await self.create(
            payload=["valid", "generic", "payload"],
            idempotency_key="automation:generic-payload",
        )
        async with self.store._db() as db:
            cursor = await db.execute(
                """
                INSERT INTO automations (
                    id,idempotency_key,name,schedule_json,job_kind,payload_json,
                    state,next_run_at,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,'enabled',?,?,?)
                """,
                (
                    "corrupt-payload",
                    "automation:corrupt-payload",
                    "ancienne ligne corrompue",
                    json.dumps(Schedule.interval(60, anchor=T0).to_payload()),
                    "run_task",
                    "{not-json",
                    datetime_to_text(T0),
                    datetime_to_text(T0),
                    datetime_to_text(T0),
                ),
            )
            corrupt_rowid = cursor.lastrowid
            await db.commit()

        self.assertEqual(await self.store.cancel_for_task(7, now=T0), 1)
        self.assertEqual(
            (await self.store.get(target.id)).state,
            AutomationState.CANCELLED,
        )
        self.assertEqual(
            (await self.store.get(unrelated.id)).state,
            AutomationState.ENABLED,
        )
        self.assertEqual(
            (await self.store.get(generic.id)).state,
            AutomationState.ENABLED,
        )
        async with self.store._db() as db:
            async with db.execute(
                "SELECT state FROM automations WHERE rowid=?", (corrupt_rowid,)
            ) as cursor:
                corrupt = await cursor.fetchone()
        self.assertEqual(corrupt["state"], AutomationState.CANCELLED.value)


if __name__ == "__main__":
    unittest.main()
