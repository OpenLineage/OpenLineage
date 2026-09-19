# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import ssl
import threading
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

import attr
from openlineage.client import event_v2
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient
    from nats.js.client import JetStreamContext
    from openlineage.client.client import Event

log = logging.getLogger(__name__)

_T = TypeVar("_T", bound="NatsConfig")
_R = TypeVar("_R")

_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _split_urls(value: str | list[str]) -> list[str]:
    urls = value.split(",") if isinstance(value, str) else value
    return [url.strip() for url in urls if url.strip()]


@attr.define
class NatsConfig(Config):
    # NATS server URL(s): a list, or a comma-separated string such as "nats://a:4222,nats://b:4222"
    url: list[str] = attr.field(converter=_split_urls)

    # Subject on which events are published
    subject: str

    # Publish through JetStream and wait for the stream's acknowledgement. When false, events are
    # published over core NATS, which drops them if no subscriber is listening at that moment.
    jetstream: bool = True

    # Seconds to wait for the JetStream ack (or for the core NATS flush)
    publishTimeout: float = 5.0  # noqa: N815
    connectTimeout: int = 5  # noqa: N815

    # Sets the Nats-Msg-Id header so JetStream's duplicate window drops retried publishes
    msgIdHeader: bool = True  # noqa: N815

    # Per-message TTL in seconds. JetStream only; the stream must be created with per-message TTL enabled.
    messageTtl: float | None = None  # noqa: N815

    # Authentication: use at most one of user/password, token, nkeysSeed or credsFile
    user: str | None = None
    password: str | None = None
    token: str | None = None
    nkeysSeed: str | None = None  # noqa: N815
    credsFile: str | None = None  # noqa: N815

    tlsCaFile: str | None = None  # noqa: N815
    tlsCertFile: str | None = None  # noqa: N815
    tlsKeyFile: str | None = None  # noqa: N815

    @classmethod
    def from_dict(cls: type[_T], params: dict[str, Any]) -> _T:
        # Env vars arrive lower-cased: OPENLINEAGE__TRANSPORT__PUBLISH_TIMEOUT -> publish_timeout
        params = {**params}
        for field in attr.fields(cls):
            snake = _CAMEL_BOUNDARY.sub("_", field.name).lower()
            if snake != field.name and snake in params:
                value = params.pop(snake)
                params.setdefault(field.name, value)
        for required in ("url", "subject"):
            if not params.get(required):
                msg = f"nats `{required}` not passed to NatsConfig"
                raise RuntimeError(msg)
        config = cls(**get_only_specified_fields(cls, params))
        config._validate_auth()
        return config

    def _validate_auth(self) -> None:
        methods = [
            name
            for name, is_set in (
                ("user/password", self.user is not None or self.password is not None),
                ("token", self.token is not None),
                ("nkeysSeed", self.nkeysSeed is not None),
                ("credsFile", self.credsFile is not None),
            )
            if is_set
        ]
        if len(methods) > 1:
            msg = f"NatsConfig accepts one authentication method, got: {', '.join(methods)}"
            raise RuntimeError(msg)
        if self.tlsKeyFile and not self.tlsCertFile:
            msg = "NatsConfig `tlsKeyFile` requires `tlsCertFile`"
            raise RuntimeError(msg)
        if self.messageTtl is not None and not self.jetstream:
            msg = "NatsConfig `messageTtl` requires `jetstream: true`"
            raise RuntimeError(msg)


def message_id(event: Event) -> str | None:
    """Deterministic id for JetStream de-duplication: identical retries of one event share it."""
    if isinstance(event, (RunEvent, event_v2.RunEvent)):
        event_type = getattr(event.eventType, "value", event.eventType)
        return f"run:{event.run.runId}:{event_type}:{event.eventTime}"
    if isinstance(event, (JobEvent, event_v2.JobEvent)):
        return f"job:{event.job.namespace}/{event.job.name}:{event.eventTime}"
    if isinstance(event, (DatasetEvent, event_v2.DatasetEvent)):
        return f"dataset:{event.dataset.namespace}/{event.dataset.name}:{event.eventTime}"
    return None


class NatsTransport(Transport):
    kind = "nats"
    config_class = NatsConfig

    def __init__(self, config: NatsConfig) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._reset()
        log.debug(
            "Constructing OpenLineage transport that will send events to NATS subject `%s` (jetstream=%s)",
            config.subject,
            config.jetstream,
        )

    def _reset(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._pid = os.getpid()

    def emit(self, event: Event) -> None:
        payload = Serde.to_json(event).encode("utf-8")
        headers: dict[str, str] | None = None
        if self.config.jetstream and self.config.msgIdHeader and (msg_id := message_id(event)):
            headers = {"Nats-Msg-Id": msg_id}
        self._run(
            self._publish(payload, headers),
            timeout=self.config.connectTimeout + self.config.publishTimeout + 1,
        )

    def close(self, timeout: float = -1) -> bool:
        with self._lock:
            loop, thread = self._loop, self._thread
            if loop is None or thread is None or self._pid != os.getpid():
                self._reset()
                return True
            drained = True
            try:
                future = asyncio.run_coroutine_threadsafe(self._drain(), loop)
                future.result(timeout=None if timeout < 0 else timeout)
            except Exception:
                log.warning("Failed to drain NATS connection", exc_info=True)
                drained = False
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=None if timeout < 0 else timeout)
            self._reset()
            return drained

    def _run(self, coro: Coroutine[Any, Any, _R], timeout: float) -> _R:
        loop = self._ensure_loop()
        return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=timeout)

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            # A forked worker (e.g. Airflow task) inherits the loop object but not its thread.
            if self._pid != os.getpid():
                self._reset()
            if self._loop is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=self._run_loop, args=(loop,), name="openlineage-nats", daemon=True
                )
                thread.start()
                self._loop, self._thread = loop, thread
            return self._loop

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            loop.close()

    async def _publish(self, payload: bytes, headers: dict[str, str] | None) -> None:
        nc = await self._connection()
        if self.config.jetstream:
            assert self._js is not None
            ack = await self._js.publish(
                self.config.subject,
                payload,
                timeout=self.config.publishTimeout,
                headers=headers,
                msg_ttl=self.config.messageTtl,
            )
            log.debug(
                "Published event to stream %s, seq %s, duplicate=%s", ack.stream, ack.seq, ack.duplicate
            )
        else:
            await nc.publish(self.config.subject, payload)
            await nc.flush(timeout=math.ceil(self.config.publishTimeout))

    async def _connection(self) -> NatsClient:
        if self._nc is not None and not self._nc.is_closed:
            return self._nc
        try:
            import nats
        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not find nats-py module. "
                "Installing it is required for NatsTransport to work. "
                "You can also get it via `pip install openlineage-python[nats]`",
            )
            raise
        self._nc = await nats.connect(servers=self.config.url, **self._connect_options())
        self._js = self._nc.jetstream() if self.config.jetstream else None
        return self._nc

    def _connect_options(self) -> dict[str, Any]:
        c = self.config
        options: dict[str, Any] = {
            "name": "openlineage-python",
            "connect_timeout": c.connectTimeout,
            "user": c.user,
            "password": c.password,
            "token": c.token,
            "nkeys_seed": c.nkeysSeed,
            "user_credentials": c.credsFile,
        }
        if c.tlsCaFile or c.tlsCertFile:
            context = ssl.create_default_context(cafile=c.tlsCaFile)
            if c.tlsCertFile:
                context.load_cert_chain(c.tlsCertFile, c.tlsKeyFile)
            options["tls"] = context
        return {key: value for key, value in options.items() if value is not None}

    async def _drain(self) -> None:
        if self._nc is not None and not self._nc.is_closed:
            await self._nc.drain()
