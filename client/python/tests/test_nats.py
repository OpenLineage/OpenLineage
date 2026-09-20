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
import sys
import textwrap
import threading
import time
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import attr
import pytest
from openlineage.client import OpenLineageClient, event_v2
from openlineage.client.run import Dataset, DatasetEvent, Job, JobEvent, Run, RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport import get_default_factory
from openlineage.client.transport.composite import CompositeConfig, CompositeTransport
from openlineage.client.transport.nats import NatsConfig, NatsTransport, message_id
from openlineage.client.uuid import generate_new_uuid

from tests import nats_server

nats = pytest.importorskip("nats")

from nats.js.api import StreamConfig  # noqa: E402

# Short timeouts for tests that expect a failure
FAST: dict[str, Any] = {"connectTimeout": 1, "publishTimeout": 1}


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


def _payload(event: Any) -> bytes:
    return Serde.to_json(event).encode("utf-8")


def test_message_id_for_run_events_keeps_run_id_and_event_type(event: RunEvent) -> None:
    v2_event = event_v2.RunEvent(
        eventType=event_v2.RunState.COMPLETE,
        eventTime="2026-01-01T00:00:00Z",
        run=event_v2.Run(runId=event.run.runId),
        job=event_v2.Job(namespace="nats", name="test"),
    )

    assert message_id(event, _payload(event)).startswith(f"{event.run.runId}:START:")
    assert message_id(v2_event, _payload(v2_event)).startswith(f"{event.run.runId}:COMPLETE:")


def test_message_id_is_stable_for_retries_and_distinct_for_different_payloads(event: RunEvent) -> None:
    other = attr.evolve(event, eventType=RunState.OTHER)
    other_with_more_facets = attr.evolve(other, producer="another-producer")

    assert message_id(event, _payload(event)) == message_id(event, _payload(event))
    assert message_id(other, _payload(other)) != message_id(
        other_with_more_facets, _payload(other_with_more_facets)
    )


def test_message_id_for_job_and_dataset_events_does_not_depend_on_name_splitting() -> None:
    def job_event(namespace: str, name: str) -> JobEvent:
        return JobEvent(
            eventTime="2026-01-01T00:00:00Z",
            job=Job(namespace=namespace, name=name),
            producer="p",
            schemaURL="s",
        )

    first, second = job_event("a", "b/c"), job_event("a/b", "c")
    dataset_event = DatasetEvent(
        eventTime="2026-01-01T00:00:00Z",
        dataset=Dataset(namespace="ns", name="täble\r\nX: 1"),
        producer="p",
        schemaURL="s",
    )

    assert message_id(first, _payload(first)) != message_id(second, _payload(second))
    assert message_id(first, _payload(first)).startswith("job:")
    dataset_id = message_id(dataset_event, _payload(dataset_event))
    assert dataset_id.startswith("dataset:")
    assert dataset_id.isascii()
    assert "\n" not in dataset_id


def test_message_id_falls_back_to_a_digest_for_other_payloads() -> None:
    # Producers that already hold OpenLineage JSON emit it as a dict; it still de-duplicates
    payload = b'{"eventType": "START"}'

    assert message_id({"eventType": "START"}, payload).startswith("event:")
    assert message_id({"eventType": "START"}, payload) == message_id({"eventType": "START"}, payload)
    assert message_id({"eventType": "START"}, payload) != message_id(
        {"eventType": "OTHER"}, b'{"eventType": "OTHER"}'
    )


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


def test_config_converts_json_decoded_env_values_to_strings() -> None:
    config = NatsConfig.from_dict({"url": "nats://a:4222", "subject": 123, "user": 42, "password": 12345})

    assert config.subject == "123"
    assert config.user == "42"
    assert config.password == "12345"


@pytest.mark.parametrize("ttl", [0.5, 1.9, 0, -5])
def test_config_rejects_message_ttl_that_is_not_whole_seconds(ttl: float) -> None:
    with pytest.raises(RuntimeError, match="messageTtl"):
        NatsConfig.from_dict({"url": "nats://a:4222", "subject": "ol", "messageTtl": ttl})


def test_config_repr_hides_secrets() -> None:
    config = NatsConfig.from_dict(
        {"url": "nats://a:4222", "subject": "ol", "user": "u", "password": "hunter2"}
    )
    token_config = NatsConfig.from_dict({"url": "nats://a:4222", "subject": "ol", "token": "s3cret"})

    assert "hunter2" not in repr(config)
    assert "s3cret" not in repr(token_config)


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
    """A JetStream-enabled NATS server: NATS_URL if set, else a local nats-server binary, else Docker."""
    if url := os.environ.get("NATS_URL"):
        _wait_for_jetstream(url)
        yield url
        return
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
    assert message.headers["Nats-Msg-Id"] == message_id(event, _payload(event))


def test_jetstream_drops_duplicate_publishes(nats_url: str, stream: tuple[str, str], event: RunEvent) -> None:
    stream_name, subject = stream
    transport = _transport(nats_url, subject)

    transport.emit(event)
    transport.emit(event)
    transport.close()

    assert len(_stream_messages(nats_url, stream_name)) == 1


def test_jetstream_keeps_each_event_type_of_a_run(
    nats_url: str, stream: tuple[str, str], event: RunEvent
) -> None:
    # Same run and eventTime: only eventType tells START, RUNNING, COMPLETE and FAIL apart
    stream_name, subject = stream
    transport = _transport(nats_url, subject)

    for state in (RunState.START, RunState.RUNNING, RunState.COMPLETE, RunState.FAIL):
        transport.emit(attr.evolve(event, eventType=state))
    transport.close()

    event_types = [
        json.loads(message.data)["eventType"] for message in _stream_messages(nats_url, stream_name)
    ]
    assert event_types == ["START", "RUNNING", "COMPLETE", "FAIL"]


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


def _nats_transport(url: str, subject: str, **extra: Any) -> NatsTransport:
    return NatsTransport(NatsConfig.from_dict({"url": url, "subject": subject, **extra}))


def test_nkey_seed_authentication(tmp_path: Path, event: RunEvent) -> None:
    import nkeys

    user = nats_server.new_keypair(nkeys.PREFIX_BYTE_USER)
    intruder = nats_server.new_keypair(nkeys.PREFIX_BYTE_USER)
    (tmp_path / "user.nk").write_bytes(user.seed)  # type: ignore[attr-defined]
    (tmp_path / "intruder.nk").write_bytes(intruder.seed)  # type: ignore[attr-defined]
    config = f"authorization {{ users = [ {{ nkey: {user.public_key.decode()} }} ] }}\n"  # type: ignore[attr-defined]

    with nats_server.nats_server(tmp_path, config=config, jetstream=False) as server:
        allowed = _nats_transport(server.url, "ol.nkey", jetstream=False, nkeysSeed=str(tmp_path / "user.nk"))
        allowed.emit(event)
        allowed.close(5)

        denied = _nats_transport(
            server.url, "ol.nkey", jetstream=False, nkeysSeed=str(tmp_path / "intruder.nk"), **FAST
        )
        with pytest.raises(Exception):  # noqa: B017 - nats-py raises its own error types
            denied.emit(event)
        denied.close(5)


def test_creds_file_authentication(tmp_path: Path, event: RunEvent) -> None:
    trusted = nats_server.operator_setup()
    untrusted = nats_server.operator_setup()
    (tmp_path / "trusted.creds").write_text(trusted.creds)
    (tmp_path / "untrusted.creds").write_text(untrusted.creds)

    with nats_server.nats_server(tmp_path, config=trusted.server_config, jetstream=False) as server:
        allowed = _nats_transport(
            server.url, "ol.creds", jetstream=False, credsFile=str(tmp_path / "trusted.creds")
        )
        allowed.emit(event)
        allowed.close(5)

        denied = _nats_transport(
            server.url, "ol.creds", jetstream=False, credsFile=str(tmp_path / "untrusted.creds"), **FAST
        )
        with pytest.raises(Exception):  # noqa: B017 - nats-py raises its own error types
            denied.emit(event)
        denied.close(5)


def test_tls_verifies_server_against_configured_ca(tmp_path: Path, event: RunEvent) -> None:
    certs = nats_server.certificates(tmp_path)
    tls = ("--tls", "--tlscert", str(certs.server_cert), "--tlskey", str(certs.server_key))

    with nats_server.nats_server(tmp_path, *tls, jetstream=False) as server:
        trusted = _nats_transport(server.url, "ol.tls", jetstream=False, tlsCaFile=str(certs.ca))
        trusted.emit(event)
        trusted.close(5)

        # The system trust store does not know the throwaway CA
        untrusted = _nats_transport(
            server.url.replace("nats://", "tls://"), "ol.tls", jetstream=False, **FAST
        )
        with pytest.raises(Exception):  # noqa: B017 - nats-py raises its own error types
            untrusted.emit(event)
        untrusted.close(5)


def test_mutual_tls_sends_client_certificate(tmp_path: Path, event: RunEvent) -> None:
    certs = nats_server.certificates(tmp_path)
    mtls = (
        "--tlsverify",
        "--tlscert", str(certs.server_cert),
        "--tlskey", str(certs.server_key),
        "--tlscacert", str(certs.ca),
    )  # fmt: skip

    with nats_server.nats_server(tmp_path, *mtls, jetstream=False) as server:
        with_cert = _nats_transport(
            server.url,
            "ol.mtls",
            jetstream=False,
            tlsCaFile=str(certs.ca),
            tlsCertFile=str(certs.client_cert),
            tlsKeyFile=str(certs.client_key),
        )
        with_cert.emit(event)
        with_cert.close(5)

        without_cert = _nats_transport(
            server.url, "ol.mtls", jetstream=False, tlsCaFile=str(certs.ca), **FAST
        )
        with pytest.raises(Exception):  # noqa: B017 - nats-py raises its own error types
            without_cert.emit(event)
        without_cert.close(5)


def test_emit_fails_fast_when_server_is_down_and_never_publishes_late(
    tmp_path: Path, event: RunEvent
) -> None:
    port = nats_server.free_port()
    transport = _nats_transport(f"nats://127.0.0.1:{port}", "ol.down", **FAST)

    started = time.monotonic()
    with pytest.raises(Exception):  # noqa: B017 - connection errors surface as nats-py or timeout errors
        transport.emit(event)
    assert time.monotonic() - started < FAST["connectTimeout"] + FAST["publishTimeout"] + 2

    # A failed emit must stay failed: nothing may be published once the server appears
    with nats_server.nats_server(tmp_path, port=port) as server:
        nats_server.add_stream(server.url, "DOWN", ["ol.down"])
        time.sleep(3)
        assert nats_server.stream_count(server.url, "DOWN") == 0
    transport.close(5)


def test_emit_succeeds_after_server_restart(tmp_path: Path, event: RunEvent) -> None:
    port = nats_server.free_port()
    store = tmp_path / "store"
    transport = _nats_transport(f"nats://127.0.0.1:{port}", "ol.restart", msgIdHeader=False)

    with nats_server.nats_server(tmp_path, port=port, store=store) as server:
        nats_server.add_stream(server.url, "RESTART", ["ol.restart"])
        transport.emit(event)
    with nats_server.nats_server(tmp_path, port=port, store=store) as server:
        nats_server.wait_for_jetstream(server.url)
        transport.emit(event)
        transport.close(5)
        assert nats_server.stream_count(server.url, "RESTART") == 2


def test_composite_transport_continues_when_nats_fails(nats_url: str, event: RunEvent) -> None:
    composite = CompositeTransport(
        CompositeConfig.from_dict(
            {
                "transports": [
                    {"type": "nats", "url": nats_url, "subject": "openlineage.unbound", **FAST},
                    {"type": "tests.transport.AccumulatingTransport"},
                ],
                "continue_on_failure": True,
            }
        )
    )

    composite.emit(event)

    nats_transport, accumulating = composite.transports
    assert accumulating.events == [event]  # type: ignore[attr-defined]
    nats_transport.close(5)


def test_msg_id_header_cannot_inject_headers(nats_url: str, stream: tuple[str, str]) -> None:
    stream_name, subject = stream
    job_event = JobEvent(
        eventTime="2026-01-01T00:00:00Z",
        job=Job(namespace="ns", name="job\r\nX-Injected: yes"),
        producer="p",
        schemaURL="s",
    )
    transport = _nats_transport(nats_url, subject)

    transport.emit(job_event)
    transport.close(5)

    [message] = _stream_messages(nats_url, stream_name)
    assert "X-Injected" not in (message.headers or {})
    assert json.loads(message.data)["job"]["name"] == "job\r\nX-Injected: yes"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_forked_child_publishes_through_its_own_connection(
    nats_url: str, stream: tuple[str, str], event: RunEvent
) -> None:
    stream_name, subject = stream
    transport = _nats_transport(nats_url, subject, msgIdHeader=False)
    transport.emit(event)  # the parent now owns a connection and a loop thread

    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        try:
            transport.emit(event)
            transport.close(5)
            os._exit(0)
        except BaseException:
            os._exit(1)
    _, status = os.waitpid(pid, 0)
    transport.close(5)

    assert os.waitstatus_to_exitcode(status) == 0
    assert len(_stream_messages(nats_url, stream_name)) == 2


def test_transport_package_imports_without_nats_extra() -> None:
    script = textwrap.dedent(
        """
        import importlib.abc, sys

        class BlockNats(importlib.abc.MetaPathFinder):
            def find_spec(self, name, path, target=None):
                if name == "nats" or name.startswith("nats."):
                    raise ModuleNotFoundError(f"No module named {name!r}", name=name)
                return None

        sys.meta_path.insert(0, BlockNats())
        from openlineage.client.transport import get_default_factory
        assert "nats" not in sys.modules
        transport = get_default_factory().create(
            {"type": "nats", "url": "nats://127.0.0.1:1", "subject": "ol"}
        )
        from openlineage.client.run import Job, Run, RunEvent, RunState
        event = RunEvent(
            eventType=RunState.START,
            eventTime="2026-01-01T00:00:00Z",
            run=Run(runId="0199b8a0-0000-7000-8000-000000000000"),
            job=Job(namespace="ns", name="job"),
            producer="p",
            schemaURL="s",
        )
        try:
            transport.emit(event)
        except ModuleNotFoundError:
            pass
        else:
            raise AssertionError("emit should fail without nats-py")
        """
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "openlineage-python[nats]" in result.stderr


def test_concurrent_first_emits_share_one_connection(tmp_path: Path, event: RunEvent) -> None:
    import urllib.request

    monitor_port = nats_server.free_port()
    with nats_server.nats_server(tmp_path, "-m", str(monitor_port), jetstream=False) as server:
        transport = _nats_transport(server.url, "ol.concurrent", jetstream=False)
        threads = [threading.Thread(target=transport.emit, args=(event,)) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        with urllib.request.urlopen(f"http://127.0.0.1:{monitor_port}/connz") as response:
            connections = json.load(response)["num_connections"]
        transport.close(5)

    assert connections == 1
