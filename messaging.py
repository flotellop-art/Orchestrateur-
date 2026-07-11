"""Notifications sortantes par canaux configures exclusivement cote serveur.

L'entree agent ne contient que ``channel`` et ``text``. Les jetons, identifiants
Telegram et webhooks sont resolus depuis des variables d'environnement nommees
par la configuration serveur. Les destinations sont validees contre une liste
fermee et les redirections HTTP sont refusees.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol


log = logging.getLogger(__name__)

CONFIG_ENV = "ORCHESTRATOR_MESSAGING_CHANNELS"
ALLOWLIST_ENV = "ORCHESTRATOR_MESSAGING_ALLOWED_CHANNELS"
MAX_CONFIG_BYTES = 32 * 1024
MAX_CHANNELS = 64
MAX_MESSAGE_CHARACTERS = 1900
MAX_MESSAGE_BYTES = 4 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
_CHANNEL_RE = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z", re.ASCII)
_ENV_NAME_RE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z", re.ASCII)
_TELEGRAM_TOKEN_RE = re.compile(r"[0-9]{5,}:[A-Za-z0-9_-]{20,}\Z", re.ASCII)
_TELEGRAM_CHAT_RE = re.compile(r"-?[0-9]{1,20}\Z", re.ASCII)
_SLACK_PATH_RE = re.compile(
    r"/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+\Z", re.ASCII
)
_DISCORD_PATH_RE = re.compile(
    r"/api/webhooks/[0-9]+/[A-Za-z0-9_-]+\Z", re.ASCII
)


class MessagingError(RuntimeError):
    pass


class MessagingConfigurationError(MessagingError):
    pass


class MessageValidationError(MessagingError, ValueError):
    pass


class DeliveryError(MessagingError):
    pass


class ChannelKind(str, Enum):
    TELEGRAM = "telegram"
    SLACK = "slack"
    DISCORD = "discord"


@dataclass(frozen=True, slots=True)
class MessageRequest:
    channel: str
    text: str = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.channel, str) or not _CHANNEL_RE.fullmatch(self.channel):
            raise MessageValidationError("channel est invalide.")
        if not isinstance(self.text, str) or not self.text.strip():
            raise MessageValidationError("text est obligatoire.")
        if len(self.text) > MAX_MESSAGE_CHARACTERS:
            raise MessageValidationError("Le message est trop long.")
        if len(self.text.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise MessageValidationError("Le message est trop volumineux.")
        if "\x00" in self.text:
            raise MessageValidationError("Le message contient un caractere interdit.")

    @classmethod
    def from_agent_payload(cls, payload: Mapping[str, Any]) -> "MessageRequest":
        if not isinstance(payload, Mapping):
            raise MessageValidationError("La demande doit etre un objet.")
        unknown = set(payload) - {"channel", "text"}
        if unknown:
            raise MessageValidationError(
                "La demande contient des champs interdits : " + ", ".join(sorted(unknown))
            )
        return cls(channel=payload.get("channel"), text=payload.get("text"))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    body: bytes = field(default=b"", repr=False)


class HttpTransport(Protocol):
    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        timeout: float,
        max_response_bytes: int,
    ) -> HttpResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class UrllibTransport:
    """Transport standard : POST JSON, sans redirection et avec lecture bornee."""

    async def post_json(
        self,
        url: str,
        payload: Mapping[str, Any],
        *,
        timeout: float,
        max_response_bytes: int,
    ) -> HttpResponse:
        return await asyncio.to_thread(
            self._post_json_sync,
            url,
            payload,
            timeout,
            max_response_bytes,
        )

    @staticmethod
    def _post_json_sync(
        url: str,
        payload: Mapping[str, Any],
        timeout: float,
        max_response_bytes: int,
    ) -> HttpResponse:
        data = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
                "User-Agent": "Orchestrator-Messaging/1",
            },
            method="POST",
        )
        opener = urllib.request.build_opener(_NoRedirect())
        with opener.open(request, timeout=timeout) as response:
            body = response.read(max_response_bytes + 1)
            if len(body) > max_response_bytes:
                raise OSError("response_too_large")
            return HttpResponse(status_code=response.status, body=body)


@dataclass(frozen=True, slots=True)
class _ResolvedChannel:
    name: str
    kind: ChannelKind
    endpoint: str = field(repr=False)
    telegram_chat_id: str | None = None


def _env_reference(value: object, label: str) -> str:
    if not isinstance(value, str) or not _ENV_NAME_RE.fullmatch(value):
        raise MessagingConfigurationError(f"{label} doit nommer un secret serveur.")
    return value


def _safe_webhook(value: object, kind: ChannelKind) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        raise MessagingConfigurationError("Le webhook serveur configure est invalide.")
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError:
        raise MessagingConfigurationError("Le webhook serveur configure est invalide.") from None
    try:
        port = parsed.port
    except ValueError:
        raise MessagingConfigurationError("Le webhook serveur configure est invalide.") from None
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.query
        or parsed.fragment
    ):
        raise MessagingConfigurationError("Le webhook serveur configure est invalide.")
    if kind is ChannelKind.SLACK:
        valid = parsed.hostname == "hooks.slack.com" and bool(
            _SLACK_PATH_RE.fullmatch(parsed.path)
        )
    else:
        valid = parsed.hostname == "discord.com" and bool(
            _DISCORD_PATH_RE.fullmatch(parsed.path)
        )
    if not valid:
        raise MessagingConfigurationError("Le webhook serveur configure n'est pas autorise.")
    return value


class ServerChannelRegistry:
    """Registre immuable construit au demarrage depuis la configuration serveur."""

    def __init__(self, channels: Mapping[str, _ResolvedChannel]):
        self._channels = dict(channels)

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str],
        *,
        config_env: str = CONFIG_ENV,
        allowlist_env: str = ALLOWLIST_ENV,
    ) -> "ServerChannelRegistry":
        if not isinstance(environment, Mapping):
            raise MessagingConfigurationError("L'environnement serveur est invalide.")
        raw = environment.get(config_env, "{}")
        if len(raw.encode("utf-8")) > MAX_CONFIG_BYTES:
            raise MessagingConfigurationError("La configuration des canaux est trop volumineuse.")
        try:
            config = json.loads(raw)
        except (TypeError, ValueError):
            raise MessagingConfigurationError("La configuration des canaux est invalide.") from None
        if not isinstance(config, dict) or len(config) > MAX_CHANNELS:
            raise MessagingConfigurationError("La configuration des canaux est invalide.")

        allowed_raw = environment.get(allowlist_env)
        allowed: set[str] | None = None
        if allowed_raw is not None:
            allowed = {item.strip() for item in allowed_raw.split(",") if item.strip()}
            if any(not _CHANNEL_RE.fullmatch(item) for item in allowed):
                raise MessagingConfigurationError("La liste des canaux autorises est invalide.")

        channels: dict[str, _ResolvedChannel] = {}
        for name, item in config.items():
            if not isinstance(name, str) or not _CHANNEL_RE.fullmatch(name):
                raise MessagingConfigurationError("Un nom de canal configure est invalide.")
            if allowed is not None and name not in allowed:
                continue
            channels[name] = cls._resolve_channel(name, item, environment)
        return cls(channels)

    @staticmethod
    def _resolve_channel(
        name: str,
        item: object,
        environment: Mapping[str, str],
    ) -> _ResolvedChannel:
        if not isinstance(item, dict):
            raise MessagingConfigurationError("La definition d'un canal est invalide.")
        try:
            kind = ChannelKind(item.get("type"))
        except (TypeError, ValueError):
            raise MessagingConfigurationError("Le type d'un canal est invalide.") from None

        if kind is ChannelKind.TELEGRAM:
            allowed_fields = {"type", "token_env", "chat_id"}
            if set(item) - allowed_fields:
                raise MessagingConfigurationError("La definition Telegram contient un champ interdit.")
            token_env = _env_reference(item.get("token_env"), "token_env")
            token = environment.get(token_env, "")
            chat_id = item.get("chat_id")
            if not _TELEGRAM_TOKEN_RE.fullmatch(token) or not isinstance(chat_id, str) or not _TELEGRAM_CHAT_RE.fullmatch(chat_id):
                raise MessagingConfigurationError("Les secrets Telegram serveur sont invalides.")
            return _ResolvedChannel(
                name=name,
                kind=kind,
                endpoint=f"https://api.telegram.org/bot{token}/sendMessage",
                telegram_chat_id=chat_id,
            )

        allowed_fields = {"type", "webhook_env"}
        if set(item) - allowed_fields:
            raise MessagingConfigurationError("La definition du webhook contient un champ interdit.")
        webhook_env = _env_reference(item.get("webhook_env"), "webhook_env")
        endpoint = _safe_webhook(environment.get(webhook_env, ""), kind)
        return _ResolvedChannel(name=name, kind=kind, endpoint=endpoint)

    def resolve(self, channel_name: str) -> _ResolvedChannel:
        channel = self._channels.get(channel_name)
        if channel is None:
            raise MessageValidationError("Ce canal n'est pas autorise.")
        return channel

    @property
    def channel_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._channels))


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    channel: str
    kind: ChannelKind
    status_code: int


class MessagingGateway:
    def __init__(
        self,
        registry: ServerChannelRegistry,
        *,
        transport: HttpTransport | None = None,
        timeout: float = 10.0,
    ):
        if not isinstance(registry, ServerChannelRegistry):
            raise MessagingConfigurationError("registry invalide.")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not 1 <= float(timeout) <= 30
        ):
            raise MessagingConfigurationError("timeout doit etre compris entre 1 et 30 secondes.")
        self._registry = registry
        self._transport = transport or UrllibTransport()
        self._timeout = float(timeout)

    async def send_agent_payload(self, payload: Mapping[str, Any]) -> DeliveryReceipt:
        return await self.send(MessageRequest.from_agent_payload(payload))

    async def send(self, request: MessageRequest) -> DeliveryReceipt:
        if not isinstance(request, MessageRequest):
            raise MessageValidationError("request invalide.")
        channel = self._registry.resolve(request.channel)
        if channel.kind is ChannelKind.TELEGRAM:
            body: Mapping[str, Any] = {
                "chat_id": channel.telegram_chat_id,
                "text": request.text,
                "disable_web_page_preview": True,
            }
        elif channel.kind is ChannelKind.SLACK:
            body = {"text": request.text}
        else:
            body = {
                "content": request.text,
                "allowed_mentions": {"parse": []},
            }

        response: HttpResponse | None = None
        transport_failed = False
        try:
            response = await self._transport.post_json(
                channel.endpoint,
                body,
                timeout=self._timeout,
                max_response_bytes=MAX_RESPONSE_BYTES,
            )
        except Exception:
            # Ne jamais propager l'exception du transport : elle peut contenir
            # une URL secrete dans son texte ou ses attributs.
            transport_failed = True

        success = (
            not transport_failed
            and isinstance(response, HttpResponse)
            and isinstance(response.status_code, int)
            and not isinstance(response.status_code, bool)
            and 200 <= response.status_code < 300
        )
        if not success:
            raise DeliveryError(
                f"La notification n'a pas pu etre remise au canal {channel.name!r}."
            )
        assert response is not None
        log.info(
            "Notification remise channel=%s kind=%s status=%d",
            channel.name,
            channel.kind.value,
            response.status_code,
        )
        return DeliveryReceipt(channel.name, channel.kind, response.status_code)


__all__ = [
    "ALLOWLIST_ENV",
    "CONFIG_ENV",
    "ChannelKind",
    "DeliveryError",
    "DeliveryReceipt",
    "HttpResponse",
    "HttpTransport",
    "MessageRequest",
    "MessageValidationError",
    "MessagingConfigurationError",
    "MessagingError",
    "MessagingGateway",
    "ServerChannelRegistry",
    "UrllibTransport",
]
