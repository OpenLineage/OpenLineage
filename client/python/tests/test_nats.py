# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import datetime
import json
import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest
from openlineage.client import OpenLineageClient, event_v2
from openlineage.client.run import Dataset, DatasetEvent, Job, JobEvent, Run, RunEvent, RunState
from openlineage.client.transport import get_default_factory
from openlineage.client.transport.nats import NatsConfig, NatsTransport, message_id
from openlineage.client.uuid import generate_new_uuid

nats = pytest.importorskip("nats")

from nats.js.api import StreamConfig  # noqa: E402


@pytest.fixture
def event() -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="nats", name="test"),
        producer="prod",
        schemaURL="schema",
    )


def test_config_from_dict_with_defaults() -> None:
    config = NatsConfig.from_dict({"type": "nats", "url": "nats://localhost:4222", "subject": "openlineage"})

    assert config.url == ["nats://localhost:4222"]
    assert config.subject == "openlineage"
    assert config.jetstream is True
    assert config.msgIdHeader is True
    assert config.messageTtl is None


def test_config_splits_comma_separated_urls() -> None:
    config = NatsConfig.from_dict({"url": "nats://a:4222, nats://b:4222,", "subject": "ol"})

    assert config.url == ["nats://a:4222", "nats://b:4222"]


def test_config_accepts_snake_case_keys_from_env_vars() -> None:
    config = NatsConfig.from_dict(
        {
            "url": ["nats://a:4222"],
            "subject": "ol",
            "publish_timeout": 2.5,
            "msg_id_header": False,
            "message_ttl": 60,
            "creds_file": "/etc/nats/user.creds",
        }
    )

    assert config.publishTimeout == 2.5
    assert config.msgIdHeader is False
    assert config.messageTtl == 60
    assert config.credsFile == "/etc/nats/user.creds"


@pytest.mark.parametrize("missing", ["url", "subject"])
def test_config_requires_url_and_subject(missing: str) -> None:
    params = {"url": "nats://a:4222", "subject": "ol"}
    del params[missing]

    with pytest.raises(RuntimeError, match=missing):
        NatsConfig.from_dict(params)


@pytest.mark.parametrize(
    ("extra", "error"),
    [
        ({"token": "t", "credsFile": "/c"}, "one authentication method"),
        ({"user": "u", "password": "p", "nkeysSeed": "/s"}, "one authentication method"),
        ({"tlsKeyFile": "/key.pem"}, "tlsCertFile"),
        ({"jetstream": False, "messageTtl": 10}, "jetstream"),
    ],
)
def test_config_rejects_invalid_combinations(extra: dict[str, Any], error: str) -> None:
    with pytest.raises(RuntimeError, match=error):
        NatsConfig.from_dict({"url": "nats://a:4222", "subject": "ol", **extra})


def test_factory_creates_nats_transport() -> None:
    transport = get_default_factory().create({"type": "nats", "url": "nats://a:4222", "subject": "ol"})

    assert isinstance(transport, NatsTransport)


def test_message_id_for_run_events(event: RunEvent) -> None:
    v2_event = event_v2.RunEvent(
        eventType=event_v2.RunState.COMPLETE,
        eventTime="2026-01-01T00:00:00Z",
        run=event_v2.Run(runId=event.run.runId),
        job=event_v2.Job(namespace="nats", name="test"),
    )

    assert message_id(event) == f"run:{event.run.runId}:START:{event.eventTime}"
    assert message_id(v2_event) == f"run:{event.run.runId}:COMPLETE:2026-01-01T00:00:00Z"


def test_message_id_for_job_and_dataset_events() -> None:
    job_event = JobEvent(
        eventTime="2026-01-01T00:00:00Z", job=Job(namespace="ns", name="job"), producer="p", schemaURL="s"
    )
    dataset_event = DatasetEvent(
        eventTime="2026-01-01T00:00:00Z",
        dataset=Dataset(namespace="ns", name="table"),
        producer="p",
        schemaURL="s",
    )

    assert message_id(job_event) == "job:ns/job:2026-01-01T00:00:00Z"
    assert message_id(dataset_event) == "dataset:ns/table:2026-01-01T00:00:00Z"


def test_connect_options_map_credentials() -> None:
    transport = NatsTransport(
        NatsConfig.from_dict({"url": "nats://a:4222", "subject": "ol", "user": "u", "password": "p"})
    )

    options = transport._connect_options()

    assert options["user"] == "u"
    assert options["password"] == "p"
    assert "token" not in options
    assert "tls" not in options


def test_close_without_emit_is_noop() -> None:
    transport = NatsTransport(NatsConfig.from_dict({"url": "nats://a:4222", "subject": "ol"}))

    assert transport.close() is True


def test_forked_process_gets_new_event_loop() -> None:
    transport = NatsTransport(NatsConfig.from_dict({"url": "nats://a:4222", "subject": "ol"}))
    parent_loop = transport._ensure_loop()

    with mock.patch("openlineage.client.transport.nats.os.getpid", return_value=os.getpid() + 1):
        child_loop = transport._ensure_loop()

    assert child_loop is not parent_loop
    transport.close()
    parent_loop.call_soon_threadsafe(parent_loop.stop)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_jetstream(url: str, timeout: float = 30.0) -> None:
    async def ready() -> None:
        nc = await nats.connect(url, connect_timeout=1, allow_reconnect=False)
        try:
            await nc.jetstream().account_info()
        finally:
            await nc.close()

    deadline = time.monotonic() + timeout
    while True:
        try:
            asyncio.run(ready())
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


@pytest.fixture(scope="module")
def nats_url(tmp_path_factory: pytest.TempPathFactory) -> Generator[str, None, None]:
    """A JetStream-enabled NATS server: local nats-server binary if present, else a Docker container."""
    port = _free_port()
    if binary := shutil.which("nats-server"):
        store = tmp_path_factory.mktemp("jetstream")
        process = subprocess.Popen(
            [binary, "-js", "-a", "127.0.0.1", "-p", str(port), "-sd", str(store)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            url = f"nats://127.0.0.1:{port}"
            _wait_for_jetstream(url)
            yield url
        finally:
            process.terminate()
            process.wait(timeout=10)
    elif shutil.which("docker"):
        name = f"openlineage-nats-{uuid.uuid4().hex[:8]}"
        started = subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:4222", "nats:2", "-js"],
            capture_output=True,
            check=False,
        )
        if started.returncode != 0:
            pytest.skip(f"cannot start NATS container: {started.stderr.decode()}")
        try:
            url = f"nats://127.0.0.1:{port}"
            _wait_for_jetstream(url)
            yield url
        finally:
            subprocess.run(["docker", "stop", name], capture_output=True, check=False)
    else:
        pytest.skip("needs nats-server on PATH or docker")


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


@pytest.fixture
def stream(nats_url: str) -> Generator[tuple[str, str], None, None]:
    """A fresh stream per test; yields (stream name, subject)."""
    name = f"OL_{uuid.uuid4().hex[:8]}"
    subject = f"openlineage.{name.lower()}"

    async def create() -> None:
        nc = await nats.connect(nats_url)
        await nc.jetstream().add_stream(
            StreamConfig(name=name, subjects=[subject], duplicate_window=60, allow_msg_ttl=True)
        )
        await nc.close()

    async def delete() -> None:
        nc = await nats.connect(nats_url)
        await nc.jetstream().delete_stream(name)
        await nc.close()

    _run(create())
    yield name, subject
    _run(delete())


def _stream_messages(nats_url: str, stream_name: str) -> list[Any]:
    async def read() -> list[Any]:
        nc = await nats.connect(nats_url)
        js = nc.jetstream()
        info = await js.stream_info(stream_name)
        messages = [await js.get_msg(stream_name, seq) for seq in range(1, info.state.last_seq + 1)]
        await nc.close()
        return messages

    result: list[Any] = _run(read())
    return result


def _transport(url: str, subject: str, **extra: Any) -> NatsTransport:
    return NatsTransport(NatsConfig.from_dict({"url": url, "subject": subject, **extra}))


def test_jetstream_emit_persists_event_with_msg_id(
    nats_url: str, stream: tuple[str, str], event: RunEvent
) -> None:
    stream_name, subject = stream
    transport = _transport(nats_url, subject)

    transport.emit(event)
    assert transport.close() is True

    [message] = _stream_messages(nats_url, stream_name)
    assert json.loads(message.data)["run"]["runId"] == event.run.runId
    assert message.headers["Nats-Msg-Id"] == message_id(event)


def test_jetstream_drops_duplicate_publishes(nats_url: str, stream: tuple[str, str], event: RunEvent) -> None:
    stream_name, subject = stream
    transport = _transport(nats_url, subject)

    transport.emit(event)
    transport.emit(event)
    transport.close()

    assert len(_stream_messages(nats_url, stream_name)) == 1


def test_jetstream_keeps_duplicates_without_msg_id_header(
    nats_url: str, stream: tuple[str, str], event: RunEvent
) -> None:
    stream_name, subject = stream
    transport = _transport(nats_url, subject, msgIdHeader=False)

    transport.emit(event)
    transport.emit(event)
    transport.close()

    assert len(_stream_messages(nats_url, stream_name)) == 2


def test_jetstream_emit_sets_message_ttl(nats_url: str, stream: tuple[str, str], event: RunEvent) -> None:
    stream_name, subject = stream
    transport = _transport(nats_url, subject, messageTtl=3600)

    transport.emit(event)
    transport.close()

    [message] = _stream_messages(nats_url, stream_name)
    assert message.headers["Nats-TTL"] in ("3600", "3600s", "1h")


def test_jetstream_emit_raises_when_no_stream_captures_subject(nats_url: str, event: RunEvent) -> None:
    transport = _transport(nats_url, "openlineage.unbound", publishTimeout=1)

    with pytest.raises(nats.js.errors.NoStreamResponseError):
        transport.emit(event)
    transport.close()


def test_emit_after_close_reconnects(nats_url: str, stream: tuple[str, str], event: RunEvent) -> None:
    stream_name, subject = stream
    transport = _transport(nats_url, subject, msgIdHeader=False)

    transport.emit(event)
    transport.close()
    transport.emit(event)
    transport.close()

    assert len(_stream_messages(nats_url, stream_name)) == 2


def test_core_nats_emit_reaches_subscriber(nats_url: str, event: RunEvent) -> None:
    subject = f"openlineage.core.{uuid.uuid4().hex[:8]}"
    transport = _transport(nats_url, subject, jetstream=False)

    async def subscribe_then_emit() -> bytes:
        nc = await nats.connect(nats_url)
        sub = await nc.subscribe(subject)
        await nc.flush()
        await asyncio.get_running_loop().run_in_executor(None, transport.emit, event)
        message = await sub.next_msg(timeout=5)
        await nc.close()
        data: bytes = message.data
        return data

    data = _run(subscribe_then_emit())
    transport.close()

    assert json.loads(data)["run"]["runId"] == event.run.runId


def test_client_configured_from_env_vars(nats_url: str, stream: tuple[str, str], event: RunEvent) -> None:
    stream_name, subject = stream
    env = {
        "OPENLINEAGE__TRANSPORT__TYPE": "nats",
        "OPENLINEAGE__TRANSPORT__URL": nats_url,
        "OPENLINEAGE__TRANSPORT__SUBJECT": subject,
        "OPENLINEAGE__TRANSPORT__PUBLISH_TIMEOUT": "3",
    }
    with mock.patch.dict(os.environ, env):
        client = OpenLineageClient()

    assert isinstance(client.transport, NatsTransport)
    assert client.transport.config.publishTimeout == 3
    client.emit(event)
    client.transport.close()

    assert len(_stream_messages(nats_url, stream_name)) == 1
