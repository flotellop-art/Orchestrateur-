import hashlib
import json
import os
import unittest
from dataclasses import FrozenInstanceError

from install_permissions import (
    AccessLevel,
    InstallValidationError,
    build_install_argv,
    evaluate_install,
    make_install_plan,
)


def plan(manager="python", package="requests", version="2.32.3", **extra):
    payload = {"manager": manager, "package": package, "version": version}
    payload.update(extra)
    platform = "win32" if manager == "winget" else "linux"
    return make_install_plan(payload, platform=platform)


class StructuredRequestTests(unittest.TestCase):
    def test_python_and_npm_are_project_scoped(self):
        python_plan = plan()
        npm_plan = plan("npm", "vite", "5.4.8")
        self.assertEqual(python_plan.scope, "project")
        self.assertEqual(python_plan.access, AccessLevel.PROJECT)
        self.assertEqual(npm_plan.scope, "project")
        self.assertEqual(npm_plan.access, AccessLevel.PROJECT)

    def test_scoped_npm_package_is_supported(self):
        npm_plan = plan("npm", "@modelcontextprotocol/server-memory", "1.6.3")
        self.assertEqual(npm_plan.package, "@modelcontextprotocol/server-memory")

        for manager in ("python", "npm"):
            with self.subTest(manager=manager), self.assertRaisesRegex(
                InstallValidationError, "projet"
            ):
                plan(manager, scope="user")

    def test_npm_scripts_are_refused_without_isolation(self):
        with self.assertRaisesRegex(InstallValidationError, "scripts npm"):
            plan("npm", "esbuild", "0.24.0", allow_scripts=True)

    def test_winget_scope_computes_user_or_admin_access(self):
        user_plan = plan(
            "winget", "Microsoft.VisualStudioCode", "1.95.3", scope="user"
        )
        system_plan = plan("winget", "Git.Git", "2.47.0", scope="system")
        self.assertEqual(user_plan.access, AccessLevel.USER)
        self.assertEqual(system_plan.access, AccessLevel.ADMIN)
        self.assertEqual(user_plan.source, "winget")

    def test_winget_requires_scope_source_and_windows(self):
        with self.assertRaisesRegex(InstallValidationError, "scope explicite"):
            plan("winget", "Git.Git", "2.47.0")
        with self.assertRaisesRegex(InstallValidationError, "source winget"):
            plan(
                "winget",
                "Git.Git",
                "2.47.0",
                scope="user",
                source="msstore",
            )
        with self.assertRaisesRegex(InstallValidationError, "Windows"):
            make_install_plan(
                {
                    "manager": "winget",
                    "package": "Git.Git",
                    "version": "2.47.0",
                    "scope": "user",
                },
                platform="linux",
            )

    def test_rejects_unknown_manager_fields_and_non_boolean_scripts(self):
        invalid_payloads = (
            {"manager": "pip", "package": "flask", "version": "3.1.0"},
            {
                "manager": "python",
                "package": "flask",
                "version": "3.1.0",
                "options": "--user",
            },
            {
                "manager": "npm",
                "package": "vite",
                "version": "5.4.8",
                "allow_scripts": "false",
            },
            {
                "manager": "python",
                "package": "flask",
                "version": "3.1.0",
                "source": "pypi",
            },
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload), self.assertRaises(
                InstallValidationError
            ):
                make_install_plan(payload, platform="win32")

    def test_rejects_url_path_options_unicode_and_floating_versions(self):
        bad_packages = (
            "https://evil.invalid/pkg",
            "../pkg",
            "folder/pkg",
            "C:\\pkg",
            "--global",
            "pkg;calc",
            "caf\N{LATIN SMALL LETTER E WITH ACUTE}",
            "@scope/pkg",
        )
        for package in bad_packages:
            with self.subTest(package=package), self.assertRaises(
                InstallValidationError
            ):
                plan(package=package)

        bad_versions = (
            "",
            "latest",
            "*",
            "^1.2.3",
            ">=1.2.3",
            "1.2.3 --global",
            "https://evil.invalid/a.whl",
            "../1.2.3",
            "1.2.3;calc",
            "\u0661.2.3",
        )
        for version in bad_versions:
            with self.subTest(version=version), self.assertRaises(
                InstallValidationError
            ):
                plan(version=version)

    def test_accepts_strict_exact_version_forms(self):
        for version in ("1", "1.2.3", "1.0rc1", "2.0.0-beta.1", "1.0.0+cpu"):
            with self.subTest(version=version):
                self.assertEqual(plan(version=version).version, version)


class ImmutablePlanTests(unittest.TestCase):
    def test_plan_is_immutable(self):
        install_plan = plan()
        with self.assertRaises(FrozenInstanceError):
            install_plan.package = "other"
        with self.assertRaises(FrozenInstanceError):
            install_plan.plan_hash = "0" * 64

    def test_hash_is_canonical_and_binds_execution_fields(self):
        first = make_install_plan(
            {"version": "2.32.3", "package": "requests", "manager": "python"},
            platform="linux",
        )
        second = make_install_plan(
            {"manager": "python", "package": "requests", "version": "2.32.3"},
            platform="win32",
        )
        canonical = {
            "access": "project",
            "allow_scripts": False,
            "manager": "python",
            "package": "requests",
            "schema": 1,
            "scope": "project",
            "source": None,
            "version": "2.32.3",
        }
        expected = hashlib.sha256(
            json.dumps(
                canonical,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
        self.assertEqual(first.plan_hash, second.plan_hash)
        self.assertEqual(first.plan_hash, expected)
        self.assertNotEqual(first.plan_hash, plan(version="2.32.4").plan_hash)
        self.assertNotEqual(
            plan("winget", "Git.Git", "2.47.0", scope="user").plan_hash,
            plan("winget", "Git.Git", "2.47.0", scope="system").plan_hash,
        )

    def test_ui_payload_contains_only_derived_plan_information(self):
        payload = plan("npm", "vite", "5.4.8").to_payload()
        self.assertEqual(
            set(payload),
            {
                "manager",
                "package",
                "version",
                "scope",
                "access",
                "reason",
                "allow_scripts",
                "source",
                "plan_hash",
                "display",
                "command_preview",
            },
        )
        self.assertEqual(payload["display"], "vite 5.4.8 via npm")
        self.assertIsInstance(payload["command_preview"], list)
        self.assertIn("--ignore-scripts", payload["command_preview"])


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.project = plan()
        self.user = plan(
            "winget", "Microsoft.VisualStudioCode", "1.95.3", scope="user"
        )
        self.admin = plan("winget", "Git.Git", "2.47.0", scope="system")

    def test_blocked_denies_every_level(self):
        for install_plan in (self.project, self.user, self.admin):
            with self.subTest(access=install_plan.access):
                decision = evaluate_install(install_plan, "blocked")
                self.assertEqual(decision.outcome, "denied")

    def test_ask_prompts_for_project_and_denies_higher_levels(self):
        self.assertEqual(evaluate_install(self.project, "ask").outcome, "prompt")
        self.assertEqual(evaluate_install(self.user, "ask").outcome, "denied")
        self.assertEqual(evaluate_install(self.admin, "ask").outcome, "denied")

    def test_level_policies_only_automate_project_installs(self):
        expected = {
            "project": ("automatic", "denied", "denied"),
            "user": ("automatic", "prompt", "denied"),
            "admin": ("automatic", "prompt", "prompt"),
        }
        for policy, outcomes in expected.items():
            actual = tuple(
                evaluate_install(install_plan, policy).outcome
                for install_plan in (self.project, self.user, self.admin)
            )
            self.assertEqual(actual, outcomes)

    def test_decision_payload_contains_the_plan(self):
        payload = evaluate_install(self.project, "ask").to_payload()
        self.assertEqual(payload["outcome"], "prompt")
        self.assertEqual(payload["plan_hash"], self.project.plan_hash)
        self.assertEqual(payload["access"], "project")

    def test_rejects_invalid_policy(self):
        with self.assertRaises(InstallValidationError):
            evaluate_install(self.project, "always")


class CommandBuilderTests(unittest.TestCase):
    def test_python_uses_passed_venv_and_wheels_only(self):
        argv = build_install_argv(
            plan(), venv_python="C:\\Project Space\\.venv\\Scripts\\python.exe"
        )
        self.assertIsInstance(argv, tuple)
        self.assertEqual(argv[0], "C:\\Project Space\\.venv\\Scripts\\python.exe")
        self.assertEqual(argv[1:5], ("-I", "-m", "pip", "install"))
        self.assertIn("--require-virtualenv", argv)
        self.assertIn("--only-binary=:all:", argv)
        self.assertIn("--no-input", argv)
        self.assertEqual(argv[argv.index("--index-url") + 1], "https://pypi.org/simple")
        self.assertEqual(argv[-1], "requests==2.32.3")
        with self.assertRaisesRegex(InstallValidationError, "venv_python"):
            build_install_argv(plan())

    def test_npm_is_exact_and_ignores_scripts_by_default(self):
        argv = build_install_argv(plan("npm", "vite", "5.4.8"))
        self.assertEqual(argv[:3], ("npm", "install", "--save-exact"))
        self.assertIn("--registry=https://registry.npmjs.org/", argv)
        self.assertIn("--global=false", argv)
        self.assertIn("--userconfig=" + os.devnull, argv)
        self.assertIn("--package-lock=true", argv)
        self.assertIn("--ignore-scripts", argv)
        self.assertEqual(argv[-2:], ("--", "vite@5.4.8"))

    def test_npm_can_be_forced_into_a_task_owned_prefix(self):
        argv = build_install_argv(
            plan("npm", "vite", "5.4.8"), npm_prefix="C:\\task\\private-npm"
        )
        self.assertEqual(argv[argv.index("--prefix") + 1], "C:\\task\\private-npm")

    def test_winget_binds_source_id_version_scope_and_consent_flags(self):
        user_argv = build_install_argv(
            plan("winget", "Microsoft.PowerToys", "0.86.0", scope="user"),
            platform="win32",
        )
        self.assertEqual(user_argv[:4], ("winget", "install", "--source", "winget"))
        self.assertIn("--id", user_argv)
        self.assertIn("Microsoft.PowerToys", user_argv)
        self.assertIn("--version", user_argv)
        self.assertIn("0.86.0", user_argv)
        self.assertIn("--exact", user_argv)
        self.assertEqual(user_argv[user_argv.index("--scope") + 1], "user")
        self.assertIn("--accept-package-agreements", user_argv)
        self.assertIn("--accept-source-agreements", user_argv)
        self.assertIn("--disable-interactivity", user_argv)

        system_argv = build_install_argv(
            plan("winget", "Git.Git", "2.47.0", scope="system"),
            platform="win32",
        )
        self.assertEqual(system_argv[system_argv.index("--scope") + 1], "machine")

    def test_winget_builder_fails_closed_off_windows(self):
        with self.assertRaisesRegex(InstallValidationError, "Windows"):
            build_install_argv(
                plan("winget", "Git.Git", "2.47.0", scope="user"),
                platform="darwin",
            )


if __name__ == "__main__":
    unittest.main()
