import asyncio
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from agent_skills import (
    HumanApprovalRequired,
    SkillConflictError,
    SkillStateError,
    SkillStatus,
    SkillValidationError,
    approve_skill,
    export_skill_md,
    get_skill,
    init_agent_skills_db,
    list_skills,
    propose_skill,
    reject_skill,
    search_skills,
)


SUMMARY = "Vérifier un changement sans oublier les risques importants."
INSTRUCTIONS = """Commencer par lire les fichiers concernés.

1. Identifier le comportement attendu.
2. Vérifier les erreurs et les limites de sécurité.
3. Exécuter les tests pertinents et résumer les résultats.
"""


class AgentSkillsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "skills.db"
        with closing(sqlite3.connect(self.db_path)) as db:
            db.execute(
                "CREATE TABLE tasks (id INTEGER PRIMARY KEY, objective TEXT NOT NULL)"
            )
            db.executemany(
                "INSERT INTO tasks(id, objective) VALUES (?, ?)",
                ((1, "Première tâche"), (2, "Deuxième tâche")),
            )
            db.commit()
        await init_agent_skills_db(self.db_path)

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def propose(self, **changes):
        values = {
            "task_id": 1,
            "agent": "Security Agent",
            "name": "Secure Review",
            "summary": SUMMARY,
            "instructions": INSTRUCTIONS,
            "tags": ["security", "review"],
            "db_path": self.db_path,
        }
        values.update(changes)
        return await propose_skill(**values)

    async def approve(self, skill_id, **changes):
        values = {
            "approved_by": "human:florent",
            "human_confirmed": True,
            "db_path": self.db_path,
        }
        values.update(changes)
        return await approve_skill(skill_id, **values)

    async def test_init_is_idempotent_and_schema_enforces_review_metadata(self):
        await init_agent_skills_db(self.db_path)
        await init_agent_skills_db(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as db:
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("agent_skills", tables)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    """
                    INSERT INTO agent_skills
                      (task_id, proposed_by, name, slug, summary, instructions,
                       tags_json, content_hash, status, proposed_at)
                    VALUES (1,'Agent','Name','name','summary','instructions',
                            '[]','hash','active','now')
                    """
                )

    async def test_proposal_is_pending_and_must_reference_an_existing_task(self):
        skill = await self.propose()
        self.assertEqual(skill.status, SkillStatus.PENDING)
        self.assertEqual(skill.task_id, 1)
        self.assertEqual(skill.proposed_by, "Security Agent")
        self.assertEqual(skill.slug, "secure-review")
        self.assertEqual(skill.tags, ("review", "security"))
        self.assertIsNone(skill.decided_by)

        with self.assertRaisesRegex(SkillValidationError, "n'existe pas"):
            await self.propose(task_id=999, name="Other Skill")

    async def test_names_actors_and_tags_are_bounded_and_path_safe(self):
        bad_names = (
            "../escape",
            "folder/skill",
            "folder\\skill",
            "--option",
            "CON",
            "café",
            "x" * 65,
        )
        for name in bad_names:
            with self.subTest(name=name), self.assertRaises(SkillValidationError):
                await self.propose(name=name)

        for agent in ("../Agent", "Agent/Other", "Équipe", "x" * 81):
            with self.subTest(agent=agent), self.assertRaises(SkillValidationError):
                await self.propose(agent=agent)

        bad_tags = (
            ["../escape"],
            ["with space"],
            ["café"],
            ["x" * 33],
            [f"tag-{index}" for index in range(13)],
        )
        for tags in bad_tags:
            with self.subTest(tags=tags), self.assertRaises(SkillValidationError):
                await self.propose(tags=tags)

    async def test_document_limits_and_active_markup_are_rejected(self):
        bad_instructions = (
            "x" * 19,
            "Documentation normale.\x00Commande cachée.",
            "Documentation normale.\u200bTexte masqué.",
            "Documentation normale.\u202eTexte inversé dangereux.",
            "Documentation normale. <script>alert(1)</script>",
            "Documentation normale. [ouvrir](javascript:alert(1))",
            "x" * 32_769,
        )
        for instructions in bad_instructions:
            with self.subTest(sample=instructions[:30]), self.assertRaises(
                SkillValidationError
            ):
                await self.propose(instructions=instructions)

        with self.assertRaises(SkillValidationError):
            await self.propose(summary="ligne une\nligne deux")
        with self.assertRaises(SkillValidationError):
            await self.propose(summary="x" * 501)

    async def test_proposal_is_idempotent_including_after_a_decision(self):
        first = await self.propose()
        second = await self.propose(tags=["review", "security", "review"])
        self.assertEqual(first.id, second.id)
        self.assertEqual(first.content_hash, second.content_hash)
        self.assertEqual(len(await list_skills(db_path=self.db_path)), 1)

        rejected = await reject_skill(
            first.id,
            rejected_by="human:florent",
            reason="Le contenu manque de précision.",
            human_confirmed=True,
            db_path=self.db_path,
        )
        duplicate = await self.propose()
        self.assertEqual(duplicate.id, rejected.id)
        self.assertEqual(duplicate.status, SkillStatus.REJECTED)

    async def test_same_name_with_changed_content_creates_a_new_proposal(self):
        first = await self.propose()
        second = await self.propose(
            instructions=INSTRUCTIONS + "\n4. Ajouter une justification concise."
        )
        self.assertNotEqual(first.id, second.id)
        self.assertNotEqual(first.content_hash, second.content_hash)

    async def test_human_confirmation_is_required_and_only_active_is_searchable(self):
        skill = await self.propose()
        self.assertEqual(await search_skills("security", db_path=self.db_path), [])
        with self.assertRaises(HumanApprovalRequired):
            await approve_skill(
                skill.id,
                approved_by="human:florent",
                db_path=self.db_path,
            )
        stored = await get_skill(skill.id, db_path=self.db_path)
        self.assertEqual(stored.status, SkillStatus.PENDING)

        active = await self.approve(skill.id)
        self.assertEqual(active.status, SkillStatus.ACTIVE)
        self.assertEqual(active.decided_by, "human:florent")
        results = await search_skills("security", db_path=self.db_path)
        self.assertEqual([result.skill.id for result in results], [skill.id])

    async def test_approval_and_rejection_are_idempotent_terminal_transitions(self):
        approved = await self.propose(name="Approved Skill")
        first_active = await self.approve(approved.id)
        second_active = await self.approve(
            approved.id, approved_by="human:someone-else"
        )
        self.assertEqual(first_active, second_active)
        with self.assertRaises(SkillStateError):
            await reject_skill(
                approved.id,
                rejected_by="human:florent",
                reason="Trop tard pour rejeter.",
                human_confirmed=True,
                db_path=self.db_path,
            )

        rejected = await self.propose(name="Rejected Skill")
        first_rejected = await reject_skill(
            rejected.id,
            rejected_by="human:florent",
            reason="Le contenu n'est pas assez général.",
            human_confirmed=True,
            db_path=self.db_path,
        )
        second_rejected = await reject_skill(
            rejected.id,
            rejected_by="human:someone-else",
            reason="Une autre raison ne remplace pas la décision initiale.",
            human_confirmed=True,
            db_path=self.db_path,
        )
        self.assertEqual(first_rejected, second_rejected)
        with self.assertRaises(SkillStateError):
            await self.approve(rejected.id)

    async def test_same_active_name_requires_an_explicit_human_replacement(self):
        first = await self.propose()
        await self.approve(first.id)
        second = await self.propose(
            task_id=2,
            agent="Test Agent",
            instructions=INSTRUCTIONS + "\n4. Documenter les compromis retenus."
        )
        with self.assertRaises(SkillConflictError):
            await self.approve(second.id)
        stored_first = await get_skill(first.id, db_path=self.db_path)
        stored_second = await get_skill(second.id, db_path=self.db_path)
        self.assertEqual(stored_first.status, SkillStatus.ACTIVE)
        self.assertEqual(stored_second.status, SkillStatus.PENDING)

        replacement = await self.approve(second.id, replace_existing=True)
        superseded = await get_skill(first.id, db_path=self.db_path)
        self.assertEqual(replacement.status, SkillStatus.ACTIVE)
        self.assertEqual(superseded.status, SkillStatus.REJECTED)
        self.assertIn(str(second.id), superseded.decision_reason)

    async def test_listing_filters_and_bounds_are_stable(self):
        first = await self.propose(name="First Skill")
        second = await self.propose(task_id=2, agent="Other Agent", name="Second Skill")
        await self.approve(second.id)
        pending = await list_skills(status="pending", db_path=self.db_path)
        active = await list_skills(status=SkillStatus.ACTIVE, db_path=self.db_path)
        task_two = await list_skills(task_id=2, db_path=self.db_path)
        self.assertEqual([item.id for item in pending], [first.id])
        self.assertEqual([item.id for item in active], [second.id])
        self.assertEqual([item.id for item in task_two], [second.id])
        self.assertEqual(
            [item.id for item in await list_skills(limit=1, offset=0, db_path=self.db_path)],
            [second.id],
        )
        with self.assertRaises(SkillValidationError):
            await list_skills(limit=201, db_path=self.db_path)
        with self.assertRaises(SkillValidationError):
            await list_skills(status="archived", db_path=self.db_path)

    async def test_search_is_deterministic_weighted_and_active_only(self):
        exact = await self.propose(
            name="Python Review",
            summary="Vérifier proprement un changement Python important.",
            instructions=INSTRUCTIONS + "\nContrôler Python et ses tests de sécurité.",
            tags=["python", "review"],
        )
        summary_match = await self.propose(
            task_id=2,
            agent="Other Agent",
            name="Backend Check",
            summary="Relire le code Python avant sa livraison finale.",
            instructions=INSTRUCTIONS,
            tags=["backend"],
        )
        pending = await self.propose(name="Python Pending", tags=["python"])
        await self.approve(exact.id)
        await self.approve(summary_match.id)

        first = await search_skills("python review", db_path=self.db_path)
        second = await search_skills("python review", db_path=self.db_path)
        self.assertEqual(first, second)
        self.assertEqual(first[0].skill.id, exact.id)
        self.assertGreater(first[0].score, first[1].score)
        self.assertNotIn(pending.id, [result.skill.id for result in first])
        self.assertEqual(await search_skills("", db_path=self.db_path), [])
        with self.assertRaises(SkillValidationError):
            await search_skills(
                " ".join(f"mot{index}" for index in range(33)),
                db_path=self.db_path,
            )

    async def test_search_ties_are_broken_by_slug_then_id(self):
        zebra = await self.propose(name="Zebra Guide", tags=["common"])
        alpha = await self.propose(
            task_id=2, agent="Other Agent", name="Alpha Guide", tags=["common"]
        )
        await self.approve(zebra.id)
        await self.approve(alpha.id)
        results = await search_skills("common", db_path=self.db_path)
        self.assertEqual([item.skill.slug for item in results], ["alpha-guide", "zebra-guide"])

    async def test_export_is_active_fixed_atomic_and_idempotent(self):
        pending = await self.propose()
        export_root = self.root / "exports"
        with self.assertRaises(SkillStateError):
            await export_skill_md(pending.id, export_root, db_path=self.db_path)

        active = await self.approve(pending.id)
        path = await export_skill_md(active.id, export_root, db_path=self.db_path)
        self.assertEqual(
            path.resolve(),
            (export_root / "secure-review" / "SKILL.md").resolve(),
        )
        content = path.read_text(encoding="utf-8")
        self.assertIn("name: secure-review", content)
        self.assertIn(f"# {active.name}", content)
        self.assertIn(active.instructions, content)
        self.assertEqual(
            await export_skill_md(active.id, export_root, db_path=self.db_path),
            path,
        )

        path.write_text("contenu externe", encoding="utf-8")
        with self.assertRaises(SkillConflictError):
            await export_skill_md(active.id, export_root, db_path=self.db_path)
        await export_skill_md(
            active.id, export_root, overwrite=True, db_path=self.db_path
        )
        self.assertIn("name: secure-review", path.read_text(encoding="utf-8"))
        leftovers = list(path.parent.glob(".SKILL.md.*.tmp"))
        self.assertEqual(leftovers, [])

    @unittest.skipUnless(hasattr(os, "symlink"), "liens symboliques indisponibles")
    async def test_export_refuses_a_symlinked_skill_directory(self):
        skill = await self.propose()
        await self.approve(skill.id)
        export_root = self.root / "exports"
        outside = self.root / "outside"
        export_root.mkdir()
        outside.mkdir()
        link = export_root / skill.slug
        try:
            link.symlink_to(outside, target_is_directory=True)
        except OSError:
            self.skipTest("création de lien symbolique non autorisée")
        with self.assertRaises(SkillValidationError):
            await export_skill_md(skill.id, export_root, db_path=self.db_path)
        self.assertFalse((outside / "SKILL.md").exists())

    async def test_documented_code_is_never_executed(self):
        marker = self.root / "must-not-exist"
        instructions = (
            "Ce bloc est un exemple documentaire et doit rester du texte.\n\n"
            "```python\n"
            f"open({str(marker)!r}, 'w').write('executed')\n"
            "```\n"
        )
        skill = await self.propose(name="Documented Example", instructions=instructions)
        await self.approve(skill.id)
        await search_skills("documentaire", db_path=self.db_path)
        await export_skill_md(skill.id, self.root / "exports", db_path=self.db_path)
        self.assertFalse(marker.exists())

    async def test_concurrent_identical_proposals_create_one_row(self):
        proposals = await asyncio.gather(*(self.propose() for _ in range(24)))
        self.assertEqual(len({item.id for item in proposals}), 1)
        self.assertEqual(len(await list_skills(db_path=self.db_path)), 1)

    async def test_concurrent_approval_is_idempotent(self):
        skill = await self.propose()
        results = await asyncio.gather(*(self.approve(skill.id) for _ in range(16)))
        self.assertEqual({item.status for item in results}, {SkillStatus.ACTIVE})
        self.assertEqual(len({item.id for item in results}), 1)
        active = await list_skills(status="active", db_path=self.db_path)
        self.assertEqual([item.id for item in active], [skill.id])

    async def test_concurrent_versions_cannot_both_become_active(self):
        first = await self.propose()
        second = await self.propose(
            task_id=2,
            agent="Other Agent",
            instructions=INSTRUCTIONS + "\n4. Ajouter la preuve de chaque conclusion.",
        )
        outcomes = await asyncio.gather(
            self.approve(first.id), self.approve(second.id), return_exceptions=True
        )
        successes = [item for item in outcomes if not isinstance(item, Exception)]
        failures = [item for item in outcomes if isinstance(item, Exception)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], SkillConflictError)
        active = await list_skills(status="active", db_path=self.db_path)
        pending = await list_skills(status="pending", db_path=self.db_path)
        self.assertEqual(len(active), 1)
        self.assertEqual(len(pending), 1)
        self.assertEqual(active[0].slug, pending[0].slug)

    async def test_concurrent_approve_reject_has_one_terminal_winner(self):
        skill = await self.propose()

        async def approve():
            return await self.approve(skill.id)

        async def reject():
            return await reject_skill(
                skill.id,
                rejected_by="human:florent",
                reason="La proposition est refusée après examen.",
                human_confirmed=True,
                db_path=self.db_path,
            )

        outcomes = await asyncio.gather(approve(), reject(), return_exceptions=True)
        successes = [item for item in outcomes if not isinstance(item, Exception)]
        failures = [item for item in outcomes if isinstance(item, Exception)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], SkillStateError)
        final = await get_skill(skill.id, db_path=self.db_path)
        self.assertIn(final.status, {SkillStatus.ACTIVE, SkillStatus.REJECTED})


if __name__ == "__main__":
    unittest.main()
