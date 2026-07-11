import asyncio
import json
import os
import re
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
    SandboxStoppedError,
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
        self.container_names = {}
        self.command_exit_code = 0
        self.command_output = "commande ok"
        self.command_started = True
        self.command_state_error = ""
        self.timeout_on_attach = False
        self.launch_running = True
        self.stop_fails = False
        self.remove_fails = False
        self.pause_stop = False
        self.stop_started = asyncio.Event()
        self.release_stop = asyncio.Event()
        self.pause_create = False
        self.create_returncode = 0
        self.create_stdout = self.CONTAINER_ID + "\n"
        self.create_started = asyncio.Event()
        self.release_create = asyncio.Event()
        self.pause_remove = False
        self.remove_started = asyncio.Event()
        self.release_remove = asyncio.Event()
        self.extra_ps_ids = set()
        self.ps_fails = False
        self.pause_ps = False
        self.ps_started = asyncio.Event()
        self.release_ps = asyncio.Event()
        self.remove_missing_fails = False
        self.last_create = None
        self.unsafe_policy = False

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
            name = args[args.index("--name") + 1]
            self.container_names[name] = self.CONTAINER_ID
            if self.pause_create:
                self.create_started.set()
                await self.release_create.wait()
            return CliResult(
                self.create_returncode,
                self.create_stdout,
                "create refused" if self.create_returncode else "",
            )
        if args[:2] == ("start", "--attach"):
            if self.timeout_on_attach:
                raise CliTimeoutError("sortie avant délai", "")
            if not self.command_started:
                return CliResult(0, "", "")
            return CliResult(self.command_exit_code, self.command_output, "")
        if args[:1] == ("start",):
            return CliResult(0, args[-1] + "\n", "")
        if args[:1] == ("inspect",):
            format_value = args[args.index("--format") + 1]
            if format_value == "{{json .}}":
                return CliResult(0, json.dumps(self._policy_payload()), "")
            if format_value == "{{json .State}}":
                state = {
                    "Status": "exited" if self.command_started else "created",
                    "Running": False,
                    "ExitCode": self.command_exit_code,
                    "Error": self.command_state_error,
                    "StartedAt": (
                        "2026-07-11T10:00:00.000000000Z"
                        if self.command_started
                        else "0001-01-01T00:00:00Z"
                    ),
                    "FinishedAt": (
                        "2026-07-11T10:00:01.000000000Z"
                        if self.command_started
                        else "0001-01-01T00:00:00Z"
                    ),
                }
                return CliResult(0, json.dumps(state), "")
            state = "true|0|" if self.launch_running else "false|1|process exited"
            return CliResult(0, state + "\n", "")
        if args[:1] == ("logs",):
            return CliResult(0, "erreur de démarrage", "")
        if args[:1] == ("ps",):
            if self.ps_fails:
                return CliResult(1, "", "daemon unavailable")
            ids = sorted(self.containers | self.extra_ps_ids)
            filters = [
                args[index + 1]
                for index, value in enumerate(args[:-1])
                if value == "--filter"
            ]
            for value in filters:
                if value.startswith("id="):
                    expected = value.removeprefix("id=")
                    ids = [item for item in ids if item.startswith(expected)]
                elif value.startswith("name="):
                    pattern = value.removeprefix("name=")
                    matching = {
                        container_id
                        for name, container_id in self.container_names.items()
                        if re.search(pattern, "/" + name)
                    }
                    ids = [item for item in ids if item in matching]
            if self.pause_ps:
                self.ps_started.set()
                await self.release_ps.wait()
            return CliResult(0, "\n".join(ids) + ("\n" if ids else ""), "")
        if args[:1] == ("stop",):
            if self.pause_stop:
                self.stop_started.set()
                await self.release_stop.wait()
            if self.stop_fails:
                return CliResult(1, "", "stop impossible")
            return CliResult(0, args[-1] + "\n", "")
        if args[:1] == ("kill",):
            return CliResult(0, args[-1] + "\n", "")
        if args[:1] == ("rm",):
            if self.pause_remove:
                self.remove_started.set()
                await self.release_remove.wait()
            if self.remove_fails:
                return CliResult(1, "", "suppression impossible")
            identifier = args[-1]
            container_id = self.container_names.get(identifier, identifier)
            exists = (
                container_id in self.containers
                or container_id in self.extra_ps_ids
            )
            if self.remove_missing_fails and not exists:
                return CliResult(1, "", "No such container")
            self.containers.discard(container_id)
            self.extra_ps_ids.discard(container_id)
            for name, known_id in list(self.container_names.items()):
                if known_id == container_id:
                    self.container_names.pop(name, None)
            return CliResult(0, container_id + "\n", "")
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
        payload = {
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
        if self.unsafe_policy:
            payload["HostConfig"]["Privileged"] = True
        return payload


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

    async def test_run_command_refuses_false_success_when_container_never_started(self):
        self.runner.command_started = False
        runtime = self.runtime()

        with self.assertRaisesRegex(SandboxExecutionError, "pas démarré"):
            await runtime.run_command(
                "false-start", self.workspace, ("python", "script.py")
            )

        self.assertNotIn(FakeDockerRunner.CONTAINER_ID, self.runner.containers)
        self.assertTrue(self.runner.docker_calls("rm"))

    async def test_run_command_preserves_real_program_nonzero_exit(self):
        self.runner.command_exit_code = 7
        self.runner.command_output = "échec du programme"
        runtime = self.runtime()

        result = await runtime.run_command(
            "program-error", self.workspace, ("python", "script.py")
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.exit_code, 7)
        self.assertEqual(result.output, "échec du programme")

    async def test_policy_inspect_independently_rejects_privileged_container(self):
        self.runner.unsafe_policy = True
        runtime = self.runtime()

        with self.assertRaisesRegex(SandboxExecutionError, "protections demandées"):
            await runtime.run_command(
                "unsafe-policy", self.workspace, ("python", "script.py")
            )

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
        self.assertIn(FakeDockerRunner.CONTAINER_ID, runtime._containers["3"])
        self.assertIsNotNone(runtime.blocked_reason)

    async def test_concurrent_stop_calls_share_one_reliable_result(self):
        runtime = self.runtime()
        self.runner.extra_ps_ids.add(FakeDockerRunner.CONTAINER_ID)
        self.runner.pause_stop = True

        first = asyncio.create_task(runtime.stop("same-task"))
        await asyncio.wait_for(self.runner.stop_started.wait(), timeout=1)
        second = asyncio.create_task(runtime.stop("same-task"))
        await asyncio.sleep(0)
        self.runner.release_stop.set()
        first_result, second_result = await asyncio.gather(first, second)

        self.assertEqual(first_result, second_result)
        self.assertTrue(first_result.ok)
        self.assertEqual(
            len(self.runner.docker_calls("stop")),
            1,
            "les deux appels doivent partager la même opération Docker",
        )

    async def test_concurrent_stop_handle_calls_share_one_container_operation(self):
        runtime = self.runtime(max_launch_seconds=30)
        handle = await runtime.launch(
            "same-handle",
            self.workspace,
            ("python", "app.py"),
            lifetime_seconds=20,
        )
        self.runner.pause_stop = True

        first = asyncio.create_task(runtime.stop_handle(handle))
        await asyncio.wait_for(self.runner.stop_started.wait(), timeout=1)
        second = asyncio.create_task(runtime.stop_handle(handle))
        await asyncio.sleep(0)
        self.runner.release_stop.set()
        first_result, second_result = await asyncio.gather(first, second)

        self.assertEqual(first_result, second_result)
        self.assertTrue(first_result.ok)
        self.assertEqual(len(self.runner.docker_calls("stop")), 1)
        self.assertEqual(len(self.runner.docker_calls("rm")), 1)
        self.assertIsNone(runtime.blocked_reason)

    async def test_stop_handle_only_removes_its_launched_container(self):
        runtime = self.runtime(max_launch_seconds=30)
        handle = await runtime.launch(
            "two-apps", self.workspace, ("python", "app.py"), lifetime_seconds=20
        )
        sibling = "b" * 64
        self.runner.extra_ps_ids.add(sibling)
        runtime._containers["two-apps"].add(sibling)

        result = await runtime.stop_handle(handle)

        self.assertTrue(result.ok)
        self.assertEqual(result.stopped, (handle.container_id,))
        self.assertIn(sibling, self.runner.extra_ps_ids)
        self.assertIn(sibling, runtime._containers["two-apps"])
        await runtime.stop("two-apps")

    async def test_normal_cleanup_and_stop_share_one_container_operation(self):
        runtime = self.runtime()
        container_id = FakeDockerRunner.CONTAINER_ID
        runtime._containers["cleanup-race"] = {container_id}
        runtime._container_names[container_id] = "tracked"
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def controlled_cleanup(identifier):
            nonlocal calls
            self.assertEqual(identifier, container_id)
            calls += 1
            started.set()
            await release.wait()
            await runtime._unregister(identifier)
            return None

        with patch.object(
            runtime, "_perform_stop_and_remove", side_effect=controlled_cleanup
        ):
            normal = asyncio.create_task(
                runtime._cleanup_container(container_id, stop_first=False)
            )
            await started.wait()
            concurrent_stop = asyncio.create_task(
                runtime._stop_and_remove(container_id)
            )
            await asyncio.sleep(0)
            release.set()
            self.assertEqual(await normal, None)
            self.assertEqual(await concurrent_stop, None)

        self.assertEqual(calls, 1)
        self.assertIsNone(runtime.blocked_reason)

    async def test_cancelled_docker_create_cleans_container_by_known_name(self):
        runtime = self.runtime()
        self.runner.pause_create = True

        command = asyncio.create_task(
            runtime.run_command(
                "cancel-create", self.workspace, ("python", "script.py")
            )
        )
        await asyncio.wait_for(self.runner.create_started.wait(), timeout=1)
        command.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await command

        self.assertFalse(self.runner.containers)
        self.assertNotIn("cancel-create", runtime._creating)
        self.assertEqual(len(self.runner.docker_calls("rm")), 1)
        self.assertIsNone(runtime.blocked_reason)

    async def test_cancellation_during_invalid_id_cleanup_waits_for_absence(self):
        runtime = self.runtime()
        self.runner.create_stdout = "not-a-container-id\n"
        self.runner.pause_remove = True

        command = asyncio.create_task(
            runtime.run_command(
                "invalid-id-cancel", self.workspace, ("python", "script.py")
            )
        )
        await asyncio.wait_for(self.runner.remove_started.wait(), timeout=1)
        command.cancel()
        await asyncio.sleep(0)
        self.assertFalse(command.done())
        self.runner.release_remove.set()
        with self.assertRaises(asyncio.CancelledError):
            await command

        self.assertFalse(self.runner.containers)
        self.assertNotIn("invalid-id-cancel", runtime._creating)
        self.assertIsNone(runtime.blocked_reason)

    async def test_refused_create_also_cleans_the_uncertain_container(self):
        runtime = self.runtime()
        self.runner.create_returncode = 1
        self.runner.create_stdout = ""

        with self.assertRaisesRegex(SandboxExecutionError, "refusé la création"):
            await runtime.run_command(
                "refused-create", self.workspace, ("python", "script.py")
            )

        self.assertFalse(self.runner.containers)
        self.assertNotIn("refused-create", runtime._creating)

    async def test_missing_container_is_success_only_after_fresh_confirmation(self):
        runtime = self.runtime()
        container_id = FakeDockerRunner.CONTAINER_ID
        runtime._containers["already-gone"] = {container_id}
        runtime._container_names[container_id] = "already-gone"
        self.runner.remove_missing_fails = True

        error = await runtime._stop_and_remove(container_id)

        self.assertIsNone(error)
        self.assertNotIn("already-gone", runtime._containers)
        self.assertIsNone(runtime.blocked_reason)
        self.assertTrue(self.runner.docker_calls("ps"))

    async def test_missing_container_without_confirmation_stays_failed_closed(self):
        runtime = self.runtime()
        container_id = FakeDockerRunner.CONTAINER_ID
        runtime._containers["unverified-gone"] = {container_id}
        runtime._container_names[container_id] = "unverified-gone"
        self.runner.remove_missing_fails = True
        self.runner.ps_fails = True

        error = await runtime._stop_and_remove(container_id)

        self.assertIsNotNone(error)
        self.assertIn(container_id, runtime._containers["unverified-gone"])
        self.assertIsNotNone(runtime.blocked_reason)

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

    async def test_global_cleanup_barrier_refuses_creation_after_its_snapshot(self):
        runtime = self.runtime()
        self.runner.pause_ps = True

        cleanup = asyncio.create_task(runtime.stop_all_managed())
        await asyncio.wait_for(self.runner.ps_started.wait(), timeout=1)
        with self.assertRaises(SandboxStoppedError):
            await runtime.run_command(
                "too-late", self.workspace, ("python", "script.py")
            )
        self.runner.release_ps.set()
        result = await cleanup

        self.assertTrue(result.ok)
        self.assertFalse(runtime._stopping_all)
        self.assertFalse(self.runner.docker_calls("create"))

    async def test_global_cleanup_waits_for_creation_already_in_progress(self):
        runtime = self.runtime()
        self.runner.pause_create = True

        command = asyncio.create_task(
            runtime.run_command(
                "already-creating", self.workspace, ("python", "script.py")
            )
        )
        await asyncio.wait_for(self.runner.create_started.wait(), timeout=1)
        cleanup = asyncio.create_task(runtime.stop_all_managed())
        await asyncio.sleep(0)
        self.assertFalse(cleanup.done())

        self.runner.release_create.set()
        with self.assertRaises(SandboxStoppedError):
            await command
        result = await cleanup

        self.assertTrue(result.ok)
        self.assertFalse(self.runner.containers)
        self.assertFalse(runtime._stopping_all)

    async def test_concurrent_global_cleanups_share_barrier_and_result(self):
        runtime = self.runtime()
        self.runner.extra_ps_ids.add(FakeDockerRunner.CONTAINER_ID)
        self.runner.pause_stop = True

        first = asyncio.create_task(runtime.stop_all_managed())
        await asyncio.wait_for(self.runner.stop_started.wait(), timeout=1)
        second = asyncio.create_task(runtime.stop_all_managed())
        await asyncio.sleep(0)
        self.runner.release_stop.set()
        first_result, second_result = await asyncio.gather(first, second)

        self.assertEqual(first_result, second_result)
        self.assertTrue(first_result.ok)
        self.assertEqual(len(self.runner.docker_calls("ps")), 1)
        self.assertEqual(len(self.runner.docker_calls("stop")), 1)
        self.assertEqual(len(self.runner.docker_calls("rm")), 1)
        self.assertFalse(runtime._stopping_all)

    async def test_startup_cleanup_rejects_invalid_docker_ps_identifiers(self):
        runtime = self.runtime()
        self.runner.extra_ps_ids.update({
            FakeDockerRunner.CONTAINER_ID,
            "not-a-container-id",
        })

        result = await runtime.stop_all_managed(strict=False)

        self.assertFalse(result.ok)
        self.assertTrue(any(item == "recherche" for item, _ in result.failed))
        self.assertNotIn(FakeDockerRunner.CONTAINER_ID, self.runner.extra_ps_ids)
        self.assertIsNotNone(runtime.blocked_reason)

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

    async def test_handle_liveness_is_checked_against_docker_state(self):
        runtime = self.runtime()
        handle = await runtime.launch(
            "live-handle", self.workspace, ("python", "app.py")
        )
        self.assertTrue(await runtime.is_handle_running(handle))

        self.runner.launch_running = False
        self.assertFalse(await runtime.is_handle_running(handle))

        await runtime.stop_handle(handle)
        self.assertFalse(await runtime.is_handle_running(handle))

    async def test_failed_expiry_remains_tracked_and_blocks_execution(self):
        runtime = self.runtime(max_launch_seconds=0.03)
        self.runner.remove_fails = True
        handle = await runtime.launch(
            "expiry-failure",
            self.workspace,
            ("python", "app.py"),
            lifetime_seconds=0.01,
        )

        await asyncio.sleep(0.06)

        self.assertIn(handle.container_id, runtime._containers["expiry-failure"])
        self.assertIn(handle.container_id, self.runner.containers)
        self.assertIsNotNone(runtime.blocked_reason)
        with self.assertRaises(SandboxUnavailableError):
            await runtime.run_command(
                "blocked-after-expiry", self.workspace, ("python", "script.py")
            )

        self.runner.remove_fails = False
        cleanup = await runtime.stop("expiry-failure")
        self.assertTrue(cleanup.ok)
        runtime.unblock_execution()

    async def test_probe_fails_closed_for_daemon_image_version_and_os(self):
        cases = (
            ("daemon", {"daemon_available": False}, "n'est pas démarré"),
            ("image", {"image_available": False}, "n'est pas installée"),
            ("version 20.10", {"version": "20.10.27"}, "23.0"),
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

    async def test_docker_23_is_the_supported_version_boundary(self):
        for version, expected in (("22.99.9", False), ("23.0.0", True)):
            with self.subTest(version=version):
                runner = FakeDockerRunner()
                runner.version = version
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
                self.assertEqual(status.available, expected, status.message)

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
