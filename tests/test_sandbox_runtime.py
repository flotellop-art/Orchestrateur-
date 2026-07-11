import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sandbox_runtime import (
    CliResult,
    CliTimeoutError,
    DockerSandboxRuntime,
    SandboxConfig,
    SandboxExecutionError,
    SandboxStopError,
    SandboxUnavailableError,
    SandboxValidationError,
)


class FakeDockerRunner:
    """Petit moteur Docker déterministe ; aucun démon réel n'est utilisé."""

    CONTAINER_ID = "a" * 64

    def __init__(self) -> None:
        self.calls = []
        self.daemon_available = True
        self.image_available = True
        self.os_type = "linux"
        self.version = "26.1.4"
        self.containers = set()
        self.command_exit_code = 0
        self.command_output = "commande ok"
        self.timeout_on_attach = False
        self.launch_running = True
        self.stop_fails = False
        self.remove_fails = False
        self.extra_ps_ids = set()
        self.last_create = None

    async def run(self, argv, *, timeout=None):
        argv = tuple(argv)
        self.calls.append((argv, timeout))
        args = argv[1:]

        if args[:1] == ("version",):
            if not self.daemon_available:
                return CliResult(1, "", "Docker Desktop n'est pas démarré")
            return CliResult(0, self.version + "\n", "")
        if args[:1] == ("info",):
            if "{{json .Warnings}}" in args:
                return CliResult(0, "[]\n", "")
            return CliResult(0, self.os_type + "\n", "")
        if args[:2] == ("image", "inspect"):
            if not self.image_available:
                return CliResult(1, "", "No such image")
            return CliResult(0, "sha256:trusted\n", "")
        if args[:1] == ("create",):
            self.last_create = args
            self.containers.add(self.CONTAINER_ID)
            return CliResult(0, self.CONTAINER_ID + "\n", "")
        if args[:2] == ("start", "--attach"):
            if self.timeout_on_attach:
                raise CliTimeoutError("sortie avant délai", "")
            return CliResult(self.command_exit_code, self.command_output, "")
        if args[:1] == ("start",):
            return CliResult(0, args[-1] + "\n", "")
        if args[:1] == ("inspect",):
            format_value = args[args.index("--format") + 1]
            if format_value == "{{json .}}":
                return CliResult(0, json.dumps(self._policy_payload()), "")
            if "ExitCode}}" in format_value and "Running" not in format_value:
                return CliResult(0, str(self.command_exit_code) + "\n", "")
            state = "true|0|" if self.launch_running else "false|1|process exited"
            return CliResult(0, state + "\n", "")
        if args[:1] == ("logs",):
            return CliResult(0, "erreur de démarrage", "")
        if args[:1] == ("ps",):
            ids = sorted(self.containers | self.extra_ps_ids)
            return CliResult(0, "\n".join(ids) + ("\n" if ids else ""), "")
        if args[:1] == ("stop",):
            if self.stop_fails:
                return CliResult(1, "", "stop impossible")
            return CliResult(0, args[-1] + "\n", "")
        if args[:1] == ("kill",):
            return CliResult(0, args[-1] + "\n", "")
        if args[:1] == ("rm",):
            if self.remove_fails:
                return CliResult(1, "", "suppression impossible")
            identifier = args[-1]
            self.containers.discard(identifier)
            self.extra_ps_ids.discard(identifier)
            return CliResult(0, identifier + "\n", "")
        raise AssertionError(f"Appel Docker inattendu : {argv!r}")

    def docker_calls(self, operation):
        return [call for call, _ in self.calls if len(call) > 1 and call[1] == operation]

    def _policy_payload(self):
        create = self.last_create
        if create is None:
            raise AssertionError("Aucun docker create n'a précédé inspect")

        def values(option):
            return [create[index + 1] for index, value in enumerate(create) if value == option]

        def value(option):
            return values(option)[-1]

        def bytes_value(text):
            return int(text[:-1]) * {"k": 1024, "m": 1024**2, "g": 1024**3}[text[-1].lower()]

        mount_parts = dict(part.split("=", 1) for part in value("--mount").split(","))
        labels = dict(item.split("=", 1) for item in values("--label"))
        tmpfs = {}
        for item in values("--tmpfs"):
            target, options = item.split(":", 1)
            tmpfs[target] = options
        ports = {}
        for item in values("--publish"):
            host_ip, host_port, container = item.rsplit(":", 2)
            container_port = container.removesuffix("/tcp")
            ports[f"{container_port}/tcp"] = [
                {"HostIp": host_ip, "HostPort": host_port}
            ]
        entrypoint_index = create.index("--entrypoint")
        entrypoint = create[entrypoint_index + 1]
        command = list(create[entrypoint_index + 3 :])
        memory = bytes_value(value("--memory"))
        return {
            "Config": {
                "User": value("--user"),
                "Labels": labels,
                "Entrypoint": [entrypoint],
                "Cmd": command,
                "Healthcheck": {"Test": ["NONE"]},
            },
            "HostConfig": {
                "NetworkMode": value("--network"),
                "ReadonlyRootfs": "--read-only" in create,
                "Privileged": False,
                "Init": "--init" in create,
                "IpcMode": value("--ipc"),
                "CgroupnsMode": value("--cgroupns"),
                "PidMode": "",
                "UTSMode": "",
                "CapDrop": values("--cap-drop"),
                "CapAdd": None,
                "SecurityOpt": values("--security-opt"),
                "Memory": memory,
                "MemorySwap": bytes_value(value("--memory-swap")),
                "MemorySwappiness": int(value("--memory-swappiness")),
                "NanoCpus": int(float(value("--cpus")) * 1_000_000_000),
                "PidsLimit": int(value("--pids-limit")),
                "RestartPolicy": {"Name": value("--restart")},
                "AutoRemove": False,
                "Devices": None,
                "DeviceRequests": None,
                "DeviceCgroupRules": None,
                "Binds": None,
                "Mounts": [
                    {
                        "Type": mount_parts["type"],
                        "Source": mount_parts["source"],
                        "Target": mount_parts["target"],
                        "ReadOnly": False,
                    }
                ],
                "Tmpfs": tmpfs,
                "Ulimits": [{"Name": "nofile", "Soft": 1024, "Hard": 1024}],
                "LogConfig": {
                    "Type": value("--log-driver"),
                    "Config": dict(item.split("=", 1) for item in values("--log-opt")),
                },
                "PortBindings": ports,
            },
        }


class DockerSandboxRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "racine autorisee"
        self.workspace = self.root / "projet avec espaces"
        self.workspace.mkdir(parents=True)
        self.runner = FakeDockerRunner()

    async def asyncTearDown(self):
        self.temp.cleanup()

    def runtime(self, **overrides):
        values = {
            "image": "orchestrator-sandbox:test",
            "allowed_workspace_roots": (self.root,),
            "namespace": "test-suite",
            "user": "12345:12345",
        }
        values.update(overrides)
        return DockerSandboxRuntime(SandboxConfig(**values), runner=self.runner)

    async def test_run_command_builds_hardened_container_and_cleans_it(self):
        runtime = self.runtime()
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "host-secret"}):
            result = await runtime.run_command(
                42,
                self.workspace,
                ("python", "-c", "print('ok')"),
                env={"PYTHONUNBUFFERED": "1"},
            )

        self.assertTrue(result.ok)
        self.assertEqual(result.output, "commande ok")
        create = self.runner.docker_calls("create")[0]
        required_pairs = {
            ("--network", "none"),
            ("--user", "12345:12345"),
            ("--cap-drop", "ALL"),
            ("--security-opt", "no-new-privileges:true"),
            ("--pids-limit", "128"),
            ("--memory", "512m"),
            ("--memory-swap", "512m"),
            ("--ipc", "none"),
            ("--restart", "no"),
            ("--entrypoint", "python"),
        }
        for option, value in required_pairs:
            with self.subTest(option=option):
                position = create.index(option)
                self.assertEqual(create[position + 1], value)
        for flag in ("--read-only", "--init", "--pull=never"):
            self.assertIn(flag, create)
        mount = create[create.index("--mount") + 1]
        self.assertEqual(
            mount,
            f"type=bind,source={self.workspace.resolve()},target=/workspace",
        )
        self.assertNotIn("host-secret", "\n".join(create))
        self.assertIn("PYTHONUNBUFFERED=1", create)
        self.assertNotIn(FakeDockerRunner.CONTAINER_ID, self.runner.containers)
        self.assertTrue(self.runner.docker_calls("rm"))

    async def test_run_tests_uses_structured_default_command(self):
        runtime = self.runtime()
        result = await runtime.run_tests(7, self.workspace)
        self.assertTrue(result.ok)
        create = self.runner.docker_calls("create")[0]
        image_index = create.index("orchestrator-sandbox:test")
        self.assertEqual(create[create.index("--entrypoint") + 1], "python")
        self.assertEqual(create[image_index + 1 :], ("-m", "pytest", "-q"))

    async def test_timeout_stops_container_tree_and_returns_code_124(self):
        self.runner.timeout_on_attach = True
        runtime = self.runtime(default_timeout=10)
        result = await runtime.run_command(
            "task-1", self.workspace, ("python", "slow.py"), timeout=2
        )
        self.assertTrue(result.timed_out)
        self.assertEqual(result.exit_code, 124)
        self.assertIn("2 secondes", result.output)
        self.assertIn("sortie avant délai", result.output)
        self.assertTrue(self.runner.docker_calls("stop"))
        self.assertTrue(self.runner.docker_calls("rm"))

    async def test_stop_falls_back_to_kill_when_graceful_stop_fails(self):
        runtime = self.runtime()
        self.runner.extra_ps_ids.add(FakeDockerRunner.CONTAINER_ID)
        self.runner.stop_fails = True
        result = await runtime.stop("task-2")
        self.assertTrue(result.ok)
        self.assertEqual(result.stopped, (FakeDockerRunner.CONTAINER_ID,))
        self.assertTrue(self.runner.docker_calls("kill"))
        self.assertTrue(self.runner.docker_calls("rm"))
        self.assertFalse(self.runner.docker_calls("image"))

    async def test_stop_reports_container_that_cannot_be_removed(self):
        runtime = self.runtime()
        self.runner.extra_ps_ids.add(FakeDockerRunner.CONTAINER_ID)
        self.runner.remove_fails = True
        with self.assertRaises(SandboxStopError) as raised:
            await runtime.stop(3)
        self.assertFalse(raised.exception.result.ok)
        self.assertIn("suppression impossible", str(raised.exception))

    async def test_startup_cleanup_removes_all_containers_in_private_namespace(self):
        runtime = self.runtime()
        self.runner.extra_ps_ids.add(FakeDockerRunner.CONTAINER_ID)
        result = await runtime.stop_all_managed()
        self.assertTrue(result.ok)
        self.assertEqual(result.stopped, (FakeDockerRunner.CONTAINER_ID,))
        ps = self.runner.docker_calls("ps")[-1]
        self.assertIn("--no-trunc", ps)
        self.assertIn("label=com.orchestrator.sandbox=true", ps)
        self.assertIn("label=com.orchestrator.namespace=test-suite", ps)
        self.assertFalse(self.runner.extra_ps_ids)

    async def test_launch_requires_explicit_network_and_binds_loopback_only(self):
        runtime = self.runtime(allow_network=True)
        with self.assertRaisesRegex(SandboxValidationError, "autorisation réseau"):
            await runtime.launch(
                5,
                self.workspace,
                ("python", "app.py"),
                published_ports={8080: 5000},
            )

        handle = await runtime.launch(
            5,
            self.workspace,
            ("python", "app.py"),
            env={"PORT": "5000"},
            network_enabled=True,
            published_ports={8080: 5000},
            lifetime_seconds=30,
        )
        self.assertEqual(handle.published_ports, ((8080, 5000),))
        create = self.runner.docker_calls("create")[0]
        self.assertEqual(create[create.index("--network") + 1], "bridge")
        self.assertEqual(
            create[create.index("--publish") + 1], "127.0.0.1:8080:5000/tcp"
        )
        stopped = await runtime.stop(5)
        self.assertTrue(stopped.ok)

    async def test_network_is_denied_by_admin_configuration(self):
        runtime = self.runtime(allow_network=False)
        with self.assertRaisesRegex(SandboxValidationError, "administrateur"):
            await runtime.run_command(
                5,
                self.workspace,
                ("python", "script.py"),
                network_enabled=True,
            )
        self.assertFalse(self.runner.docker_calls("create"))

    async def test_failed_startup_cleanup_blocks_new_execution_fail_closed(self):
        runtime = self.runtime()
        runtime.block_execution("Nettoyage initial incomplet")
        status = await runtime.probe()
        self.assertFalse(status.available)
        with self.assertRaisesRegex(SandboxUnavailableError, "Nettoyage initial"):
            await runtime.run_command(5, self.workspace, ("python", "script.py"))
        self.assertFalse(self.runner.docker_calls("create"))
        runtime.unblock_execution()
        result = await runtime.run_command(5, self.workspace, ("python", "script.py"))
        self.assertTrue(result.ok)

    async def test_launch_failure_is_explained_and_cleaned(self):
        self.runner.launch_running = False
        runtime = self.runtime()
        with self.assertRaisesRegex(SandboxExecutionError, "erreur de démarrage"):
            await runtime.launch(8, self.workspace, ("python", "app.py"))
        self.assertFalse(self.runner.containers)

    async def test_launch_has_a_bounded_lifetime(self):
        runtime = self.runtime(max_launch_seconds=0.03)
        await runtime.launch(
            9,
            self.workspace,
            ("python", "app.py"),
            lifetime_seconds=0.02,
        )
        await asyncio.sleep(0.08)
        self.assertFalse(self.runner.containers)
        self.assertTrue(self.runner.docker_calls("stop"))

    async def test_probe_fails_closed_for_daemon_image_version_and_os(self):
        cases = (
            ("daemon", {"daemon_available": False}, "n'est pas démarré"),
            ("image", {"image_available": False}, "n'est pas installée"),
            ("version", {"version": "19.03.15"}, "20.10"),
            ("windows containers", {"os_type": "windows"}, "conteneurs Linux"),
        )
        for name, attributes, expected in cases:
            with self.subTest(name=name):
                runner = FakeDockerRunner()
                for attribute, value in attributes.items():
                    setattr(runner, attribute, value)
                runtime = DockerSandboxRuntime(
                    SandboxConfig(
                        image="orchestrator-sandbox:test",
                        allowed_workspace_roots=(self.root,),
                        namespace="test-suite",
                        user="12345:12345",
                    ),
                    runner=runner,
                )
                status = await runtime.probe()
                self.assertFalse(status.available)
                self.assertIn(expected, status.message)
                with self.assertRaises(SandboxUnavailableError):
                    await runtime.ensure_available()

    async def test_workspace_and_shell_strings_are_rejected_before_docker(self):
        runtime = self.runtime()
        outside = Path(self.temp.name) / "hors-racine"
        outside.mkdir()
        for folder in (outside, self.root):
            with self.subTest(folder=folder), self.assertRaises(
                SandboxValidationError
            ):
                await runtime.run_command(1, folder, ("python", "x.py"))
        with self.assertRaisesRegex(SandboxValidationError, "liste d'arguments"):
            await runtime.run_command(1, self.workspace, "python x.py")
        self.assertFalse(self.runner.calls)

    async def test_environment_is_allowlisted_and_limits_cannot_be_raised(self):
        runtime = self.runtime(default_timeout=10, max_launch_seconds=20)
        with self.assertRaisesRegex(SandboxValidationError, "n'est pas autorisée"):
            await runtime.run_command(
                1,
                self.workspace,
                ("python", "x.py"),
                env={"ANTHROPIC_API_KEY": "secret"},
            )
        with self.assertRaisesRegex(SandboxValidationError, "limite administrateur"):
            await runtime.run_command(
                1, self.workspace, ("python", "x.py"), timeout=11
            )
        with self.assertRaisesRegex(SandboxValidationError, "limite autorisée"):
            await runtime.launch(
                1,
                self.workspace,
                ("python", "app.py"),
                lifetime_seconds=21,
            )

    def test_root_user_and_invalid_resources_are_refused(self):
        with self.assertRaisesRegex(SandboxValidationError, "différent de root"):
            SandboxConfig(
                allowed_workspace_roots=(self.root,), namespace="test", user="0:0"
            )
        with self.assertRaises(SandboxValidationError):
            SandboxConfig(
                allowed_workspace_roots=(self.root,),
                namespace="test",
                user="1000:1000",
                pids_limit=5000,
            )


@unittest.skipUnless(
    os.getenv("ORCHESTRATOR_DOCKER_SMOKE") == "1",
    "Définir ORCHESTRATOR_DOCKER_SMOKE=1 pour tester un vrai démon Docker.",
)
class DockerSmokeTest(unittest.IsolatedAsyncioTestCase):
    async def test_real_locked_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "projects"
            workspace = root / "smoke"
            workspace.mkdir(parents=True)
            runtime = DockerSandboxRuntime(
                SandboxConfig(allowed_workspace_roots=(root,))
            )
            status = await runtime.ensure_available()
            self.assertTrue(status.available)
            result = await runtime.run_command(
                "smoke",
                workspace,
                ("python", "-c", "import os; assert os.getuid() != 0; print('ok')"),
                timeout=30,
            )
            self.assertTrue(result.ok, result.output)
            self.assertIn("ok", result.output)


if __name__ == "__main__":
    unittest.main()
