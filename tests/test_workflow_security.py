from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = tuple(sorted((ROOT / ".github" / "workflows").glob("*.yml")))
ACTION_USE_RE = re.compile(r"^\s*-\s+uses:\s+([^@\s]+)@([^\s#]+)", re.MULTILINE)


class WorkflowSecurityTests(unittest.TestCase):
    def test_third_party_actions_are_pinned_to_commit_sha(self) -> None:
        self.assertTrue(WORKFLOWS)
        for workflow in WORKFLOWS:
            text = workflow.read_text(encoding="utf-8")
            uses = ACTION_USE_RE.findall(text)
            self.assertTrue(uses, workflow.name)
            for action, reference in uses:
                with self.subTest(workflow=workflow.name, action=action):
                    self.assertRegex(reference, r"\A[0-9a-f]{40}\Z")

    def test_python_installs_require_locked_hashes(self) -> None:
        for workflow in WORKFLOWS:
            text = workflow.read_text(encoding="utf-8")
            install_lines = [
                line.strip()
                for line in text.splitlines()
                if "pip install" in line and not line.lstrip().startswith("#")
            ]
            self.assertTrue(install_lines, workflow.name)
            for line in install_lines:
                with self.subTest(workflow=workflow.name, line=line):
                    self.assertIn("--require-hashes", line)
                    self.assertIn("requirements-lock.txt", line)

    def test_ci_executes_the_secure_sse_test(self) -> None:
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("node tests/test_secure_sse.js", ci)

    def test_ci_builds_and_verifies_the_windows_desktop_package(self) -> None:
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("run: npm ci", ci)
        self.assertIn("run: npm run build", ci)
        self.assertIn("Setup $version.exe", ci)
        self.assertIn("chat-stream-utils.js", ci)
        self.assertIn('Filter "claude.exe"', ci)

    def test_local_installers_use_the_hashed_lock(self) -> None:
        for name in ("install.sh", "install_desktop.bat"):
            text = (ROOT / name).read_text(encoding="utf-8")
            with self.subTest(name=name):
                self.assertIn("--require-hashes", text)
                self.assertIn("requirements-lock.txt", text)


if __name__ == "__main__":
    unittest.main()
