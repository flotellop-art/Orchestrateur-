import json
import unittest

from messaging import (
    ChannelKind,
    DeliveryError,
    HttpResponse,
    MessageRequest,
    MessageValidationError,
    MessagingConfigurationError,
    MessagingGateway,
    ServerChannelRegistry,
    _NoRedirect,
)


TELEGRAM_TOKEN = "123456789:abcdefghijklmnopqrstuvwxyz_ABCDE"
SLACK_WEBHOOK = "https://hooks.slack.com/services/T000/B000/secret-slack"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/123456/secret-discord"


def environment(*, allowed=None):
    config = {
        "ops_telegram": {
            "type": "telegram",
            "token_env": "MSG_TELEGRAM_TOKEN",
            "chat_id": "-1001234567890",
        },
        "build_slack": {"type": "slack", "webhook_env": "MSG_SLACK_WEBHOOK"},
        "alerts_discord": {
            "type": "discord",
            "webhook_env": "MSG_DISCORD_WEBHOOK",
        },
    }
    result = {
        "ORCHESTRATOR_MESSAGING_CHANNELS": json.dumps(config),
        "MSG_TELEGRAM_TOKEN": TELEGRAM_TOKEN,
        "MSG_SLACK_WEBHOOK": SLACK_WEBHOOK,
        "MSG_DISCORD_WEBHOOK": DISCORD_WEBHOOK,
    }
    if allowed is not None:
        result["ORCHESTRATOR_MESSAGING_ALLOWED_CHANNELS"] = allowed
    return result


class FakeTransport:
    def __init__(self, response=HttpResponse(200, b"ok"), error=None):
        self.response = response
        self.error = error
        self.calls = []

    async def post_json(self, url, payload, *, timeout, max_response_bytes):
        self.calls.append((url, payload, timeout, max_response_bytes))
        if self.error:
            raise self.error
        return self.response


class RegistryTests(unittest.TestCase):
    def test_server_config_resolves_only_allowlisted_channels(self):
        registry = ServerChannelRegistry.from_environment(
            environment(allowed="build_slack,ops_telegram")
        )
        self.assertEqual(registry.channel_names, ("build_slack", "ops_telegram"))
        with self.assertRaisesRegex(MessageValidationError, "autorise"):
            registry.resolve("alerts_discord")

    def test_configuration_cannot_contain_an_agent_supplied_url(self):
        env = environment()
        env["ORCHESTRATOR_MESSAGING_CHANNELS"] = json.dumps(
            {
                "bad": {
                    "type": "slack",
                    "webhook_env": "MSG_SLACK_WEBHOOK",
                    "url": "https://evil.invalid/hook",
                }
            }
        )
        with self.assertRaisesRegex(MessagingConfigurationError, "interdit"):
            ServerChannelRegistry.from_environment(env)

    def test_webhooks_are_restricted_to_https_official_hosts(self):
        for webhook in (
            "http://hooks.slack.com/services/T/B/secret",
            "https://evil.invalid/services/T/B/secret",
            "https://hooks.slack.com.evil.invalid/services/T/B/secret",
            "https://hooks.slack.com/services/T/B/secret?redirect=1",
        ):
            with self.subTest(webhook=webhook):
                env = environment()
                env["MSG_SLACK_WEBHOOK"] = webhook
                with self.assertRaises(MessagingConfigurationError) as caught:
                    ServerChannelRegistry.from_environment(env)
                self.assertNotIn(webhook, str(caught.exception))

    def test_missing_secret_is_not_echoed_in_configuration_error(self):
        env = environment()
        env.pop("MSG_TELEGRAM_TOKEN")
        with self.assertRaises(MessagingConfigurationError) as caught:
            ServerChannelRegistry.from_environment(env)
        self.assertNotIn("MSG_TELEGRAM_TOKEN", str(caught.exception))


class MessageRequestTests(unittest.TestCase):
    def test_agent_payload_accepts_only_channel_and_text(self):
        request = MessageRequest.from_agent_payload(
            {"channel": "build_slack", "text": "Build termine"}
        )
        self.assertEqual(request.channel, "build_slack")
        self.assertNotIn("Build termine", repr(request))
        for forbidden in ("url", "webhook", "token", "chat_id"):
            with self.subTest(field=forbidden), self.assertRaises(
                MessageValidationError
            ):
                MessageRequest.from_agent_payload(
                    {
                        "channel": "build_slack",
                        "text": "Build termine",
                        forbidden: "secret-or-url",
                    }
                )

    def test_message_length_and_utf8_size_are_bounded(self):
        with self.assertRaises(MessageValidationError):
            MessageRequest("build_slack", "x" * 1901)
        with self.assertRaises(MessageValidationError):
            MessageRequest("build_slack", "\N{GRINNING FACE}" * 1900)


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.registry = ServerChannelRegistry.from_environment(environment())

    async def test_telegram_slack_and_discord_use_server_destinations(self):
        transport = FakeTransport()
        gateway = MessagingGateway(self.registry, transport=transport, timeout=7)
        telegram = await gateway.send(
            MessageRequest("ops_telegram", "Rapport disponible")
        )
        slack = await gateway.send_agent_payload(
            {"channel": "build_slack", "text": "Build termine"}
        )
        discord = await gateway.send(
            MessageRequest("alerts_discord", "Alerte controlee")
        )

        self.assertEqual(telegram.kind, ChannelKind.TELEGRAM)
        self.assertEqual(slack.kind, ChannelKind.SLACK)
        self.assertEqual(discord.kind, ChannelKind.DISCORD)
        self.assertEqual(transport.calls[0][0], f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage")
        self.assertEqual(transport.calls[0][1]["chat_id"], "-1001234567890")
        self.assertEqual(transport.calls[1][0], SLACK_WEBHOOK)
        self.assertEqual(transport.calls[1][1], {"text": "Build termine"})
        self.assertEqual(transport.calls[2][0], DISCORD_WEBHOOK)
        self.assertEqual(transport.calls[2][1]["allowed_mentions"], {"parse": []})
        self.assertTrue(all(call[2] == 7 for call in transport.calls))

    async def test_transport_exception_and_secret_never_escape(self):
        secret_error = f"connection failed for {SLACK_WEBHOOK}"
        transport = FakeTransport(error=RuntimeError(secret_error))
        gateway = MessagingGateway(self.registry, transport=transport)
        with self.assertRaises(DeliveryError) as caught:
            await gateway.send(MessageRequest("build_slack", "Message prive"))
        rendered = repr(caught.exception) + str(caught.exception)
        self.assertNotIn(SLACK_WEBHOOK, rendered)
        self.assertNotIn("Message prive", rendered)
        self.assertIsNone(caught.exception.__context__)

    async def test_non_success_status_is_reduced_to_a_safe_error(self):
        transport = FakeTransport(response=HttpResponse(500, b"secret debug body"))
        gateway = MessagingGateway(self.registry, transport=transport)
        with self.assertRaises(DeliveryError) as caught:
            await gateway.send(MessageRequest("alerts_discord", "Alerte"))
        self.assertNotIn("secret debug body", str(caught.exception))

    async def test_safe_audit_log_contains_no_message_or_endpoint(self):
        gateway = MessagingGateway(self.registry, transport=FakeTransport())
        with self.assertLogs("messaging", level="INFO") as captured:
            await gateway.send(MessageRequest("build_slack", "contenu confidentiel"))
        rendered = "\n".join(captured.output)
        self.assertIn("build_slack", rendered)
        self.assertNotIn("contenu confidentiel", rendered)
        self.assertNotIn("secret-slack", rendered)

    def test_timeout_is_bounded(self):
        for timeout in (0, 31, float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(
                MessagingConfigurationError
            ):
                MessagingGateway(self.registry, transport=FakeTransport(), timeout=timeout)

    def test_default_http_handler_refuses_redirects(self):
        handler = _NoRedirect()
        self.assertIsNone(handler.redirect_request(None, None, 302, "", {}, "https://evil.invalid"))


if __name__ == "__main__":
    unittest.main()
