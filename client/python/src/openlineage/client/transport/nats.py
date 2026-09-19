# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import logging
import math
import os
import re
import ssl
import threading
import weakref
from collections.abc import Callable, Coroutine
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


def _optional_str(value: object) -> str | None:
    # The client JSON-decodes environment variable values, so PASSWORD=12345 arrives as an int
    return None if value is None else str(value)


@attr.define
class NatsConfig(Config):
    # NATS server URL(s): a list, or a comma-separated string such as "nats://a:4222,nats://b:4222"
    url: list[str] = attr.field(converter=_split_urls)

    # Subject on which events are published
    subject: str = attr.field(converter=str)

    # Publish through JetStream and wait for the stream's acknowledgement. When false, events are
    # published over core NATS, which drops them if no subscriber is listening at that moment.
    jetstream: bool = True

    # Seconds to wait for the JetStream ack (or for the core NATS flush)
    publishTimeout: float = 5.0  # noqa: N815
    connectTimeout: int = 5  # noqa: N815

    # Sets the Nats-Msg-Id header so JetStream's duplicate window drops retried publishes
    msgIdHeader: bool = True  # noqa: N815

    # Per-message TTL in whole seconds. JetStream only; the stream must allow per-message TTL.
    messageTtl: float | None = None  # noqa: N815

    # Authentication: use at most one of user/password, token, nkeysSeed or credsFile
    user: str | None = attr.field(default=None, converter=_optional_str)
    password: str | None = attr.field(default=None, converter=_optional_str, repr=False)
    token: str | None = attr.field(default=None, converter=_optional_str, repr=False)
    nkeysSeed: str | None = attr.field(default=None, converter=_optional_str)  # noqa: N815
    credsFile: str | None = attr.field(default=None, converter=_optional_str)  # noqa: N815

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
            if params.get(required) in (None, "", []):
                msg = f"nats `{required}` not passed to NatsConfig"
                raise RuntimeError(msg)
        config = cls(**get_only_specified_fields(cls, params))
        config._validate()
        return config

    def _validate(self) -> None:
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
        match self.messageTtl:
            case None:
                pass
            case _ if not self.jetstream:
                msg = "NatsConfig `messageTtl` requires `jetstream: true`"
                raise RuntimeError(msg)
            # the server takes whole seconds; nats-py would silently truncate 1.9 to 1 and 0.5 to 0
            case int() | float() as ttl if ttl >= 1 and float(ttl).is_integer():
                pass
            case _:
                msg = (
                    f"NatsConfig `messageTtl` must be a whole number of seconds >= 1, got {self.messageTtl!r}"
                )
                raise RuntimeError(msg)


def message_id(event: Event, payload: bytes) -> str | None:
    """
    Nats-Msg-Id for JetStream de-duplication.

    Built from a digest of the serialized event, so a retried publish of the same event repeats the
    id while any two different events, including START/RUNNING/COMPLETE of one run at the same
    eventTime, get different ids. Names are left out: they can contain characters that are not
    allowed in NATS headers.
    """
    digest = hashlib.sha256(payload).hexdigest()[:32]
    match event:
        case RunEvent() | event_v2.RunEvent():
            event_type = getattr(event.eventType, "value", event.eventType)
            return f"{event.run.runId}:{event_type}:{digest}"
        case JobEvent() | event_v2.JobEvent():
            return f"job:{digest}"
        case DatasetEvent() | event_v2.DatasetEvent():
            return f"dataset:{digest}"
        case _:
            return None


def _import_nats() -> Any:
    try:
        import nats
    except ModuleNotFoundError:
        log.exception(
            "OpenLineage client did not find nats-py module. "
            "Installing it is required for NatsTransport to work. "
            "You can also get it via `pip install openlineage-python[nats]`",
        )
        raise
    return nats


async def _log_nats_error(error: Exception) -> None:
    # nats-py's default callback logs every connection error as ERROR with a traceback; emit
    # already raises, so keep these at debug
    log.debug("NATS client error: %s", error)


def _after_fork_callback(transport: weakref.ref[NatsTransport]) -> Callable[[], None]:
    # A weak reference, so registering the callback does not keep the transport alive
    def reset_in_child() -> None:
        if (instance := transport()) is not None:
            instance._after_fork()

    return reset_in_child


class NatsTransport(Transport):
    kind = "nats"
    config_class = NatsConfig

    def __init__(self, config: NatsConfig) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._reset()
        if hasattr(os, "register_at_fork"):
            # A forked worker (e.g. an Airflow task) inherits the loop object but not its thread,
            # and may inherit the lock while another thread holds it
            os.register_at_fork(after_in_child=_after_fork_callback(weakref.ref(self)))
        log.debug(
            "Constructing OpenLineage transport that will send events to NATS subject `%s` (jetstream=%s)",
            config.subject,
            config.jetstream,
        )

    def _reset(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._connecting = asyncio.Lock()

    def _after_fork(self) -> None:
        self._lock = threading.Lock()
        self._reset()

    def emit(self, event: Event) -> None:
        payload = Serde.to_json(event).encode("utf-8")
        headers: dict[str, str] | None = None
        if self.config.jetstream and self.config.msgIdHeader and (msg_id := message_id(event, payload)):
            headers = {"Nats-Msg-Id": msg_id}
        self._run(
            self._publish(payload, headers),
            timeout=self.config.connectTimeout + self.config.publishTimeout + 1,
        )

    def close(self, timeout: float = -1) -> bool:
        with self._lock:
            loop, nc = self._loop, self._nc
            self._reset()
        if loop is None:
            return True
        future = asyncio.run_coroutine_threadsafe(self._shutdown(nc), loop)
        # Stop the loop once shutdown has finished, even if this caller stops waiting earlier
        future.add_done_callback(lambda _: loop.call_soon_threadsafe(loop.stop))
        try:
            future.result(timeout=None if timeout < 0 else timeout)
        except Exception:
            log.warning("NATS connection did not close cleanly", exc_info=True)
            return False
        return True

    def _run(self, coro: Coroutine[Any, Any, _R], timeout: float) -> _R:
        future = asyncio.run_coroutine_threadsafe(coro, self._ensure_loop())
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            # Cancel, or the publish could still complete after emit reported failure
            future.cancel()
            msg = f"Publishing to NATS subject {self.config.subject!r} did not complete within {timeout}s"
            raise TimeoutError(msg) from None

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None:
                loop = asyncio.new_event_loop()
                threading.Thread(
                    target=self._run_loop, args=(loop,), name="openlineage-nats", daemon=True
                ).start()
                self._loop = loop
            return self._loop

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            loop.close()

    async def _publish(self, payload: bytes, headers: dict[str, str] | None) -> None:
        nc, js = await self._connection()
        if js is not None:
            ttl = None if self.config.messageTtl is None else int(self.config.messageTtl)
            ack = await js.publish(
                self.config.subject, payload, timeout=self.config.publishTimeout, headers=headers, msg_ttl=ttl
            )
            log.debug(
                "Published event to stream %s, seq %s, duplicate=%s", ack.stream, ack.seq, ack.duplicate
            )
        else:
            await nc.publish(self.config.subject, payload)
            await nc.flush(timeout=math.ceil(self.config.publishTimeout))

    async def _connection(self) -> tuple[NatsClient, JetStreamContext | None]:
        # One connection per transport even when several threads emit at once
        async with self._connecting:
            if self._nc is None or not self._nc.is_connected:
                if self._nc is not None and not self._nc.is_closed:
                    await self._nc.close()
                nats = _import_nats()
                # No background reconnects: a lost connection is replaced on the next emit, so a
                # failed emit never keeps retrying and publishing behind the caller's back
                self._nc = await nats.connect(servers=self.config.url, **self._connect_options())
                self._js = self._nc.jetstream() if self.config.jetstream else None
            return self._nc, self._js

    def _connect_options(self) -> dict[str, Any]:
        c = self.config
        options: dict[str, Any] = {
            "name": "openlineage-python",
            "connect_timeout": c.connectTimeout,
            "allow_reconnect": False,
            "error_cb": _log_nats_error,
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

    @staticmethod
    async def _shutdown(nc: NatsClient | None) -> None:
        if nc is None or nc.is_closed:
            return
        try:
            await nc.drain()
        except Exception:
            log.debug("Draining the NATS connection failed; closing it", exc_info=True)
            await nc.close()
