# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Helpers that run a real nats-server and create the credentials and certificates tests need."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

NATS_SERVER = shutil.which("nats-server")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until_listening(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as sock:
                # nats-server greets every client with an INFO line; a bound port alone is not readiness
                if sock.recv(4).startswith(b"INFO"):
                    return
        except OSError:
            pass
        if time.monotonic() > deadline:
            msg = f"nats-server did not start on port {port}"
            raise TimeoutError(msg)
        time.sleep(0.1)


def wait_for_jetstream(url: str, timeout: float = 30.0) -> None:
    import nats

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


@dataclass
class NatsServer:
    url: str
    port: int
    process: subprocess.Popen[bytes]

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=10)


@contextmanager
def nats_server(
    workdir: Path,
    *args: str,
    config: str | None = None,
    port: int | None = None,
    store: Path | None = None,
    jetstream: bool = True,
) -> Generator[NatsServer, None, None]:
    """Run a local nats-server; skips the test when the binary is not on PATH."""
    if NATS_SERVER is None:
        pytest.skip("needs nats-server on PATH")
    port = port or free_port()
    command = [NATS_SERVER, "-a", "127.0.0.1", "-p", str(port)]
    if jetstream:
        store = store or workdir / f"jetstream-{port}"
        command += ["-js", "-sd", str(store)]
    if config is not None:
        config_file = workdir / f"nats-{port}.conf"
        config_file.write_text(config)
        command += ["-c", str(config_file)]
    command += list(args)
    log = (workdir / f"nats-{port}.log").open("wb")
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
    server = NatsServer(url=f"nats://127.0.0.1:{port}", port=port, process=process)
    try:
        wait_until_listening(port)
        if jetstream:
            # the greeting comes before JetStream has finished enabling or recovering its store
            wait_for_jetstream(server.url)
        yield server
    finally:
        server.stop()
        log.close()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _issue_jwt(signer: object, subject: str, name: str, nats_claims: dict[str, object]) -> str:
    """Encode and sign a NATS v2 JWT the way nsc does."""
    claims: dict[str, object] = {
        "iat": int(time.time()),
        "iss": signer.public_key.decode(),  # type: ignore[attr-defined]
        "name": name,
        "sub": subject,
        "nats": {**nats_claims, "version": 2},
    }
    digest = hashlib.sha256(json.dumps(claims, separators=(",", ":")).encode()).digest()
    claims["jti"] = base64.b32encode(digest).decode().rstrip("=")
    header = _b64url(json.dumps({"typ": "JWT", "alg": "ed25519-nkey"}).encode())
    body = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    signature = signer.sign(f"{header}.{body}".encode())  # type: ignore[attr-defined]
    return f"{header}.{body}.{_b64url(signature)}"


def new_keypair(prefix: int) -> object:
    import nkeys

    return nkeys.from_seed(nkeys.encode_seed(os.urandom(32), prefix))


@dataclass
class OperatorSetup:
    server_config: str
    creds: str


def operator_setup() -> OperatorSetup:
    """An operator, one account and one user, as JWTs: the decentralised auth that .creds files belong to."""
    import nkeys

    operator = new_keypair(nkeys.PREFIX_BYTE_OPERATOR)
    account = new_keypair(nkeys.PREFIX_BYTE_ACCOUNT)
    user = new_keypair(nkeys.PREFIX_BYTE_USER)
    unlimited = {"subs": -1, "data": -1, "payload": -1}
    operator_jwt = _issue_jwt(operator, operator.public_key.decode(), "ol-operator", {"type": "operator"})
    account_jwt = _issue_jwt(
        operator,
        account.public_key.decode(),
        "ol-account",
        {
            "type": "account",
            "limits": {**unlimited, "imports": -1, "exports": -1, "wildcards": True, "conn": -1, "leaf": -1},
        },
    )
    user_jwt = _issue_jwt(
        account, user.public_key.decode(), "ol-user", {"type": "user", "pub": {}, "sub": {}, **unlimited}
    )
    server_config = (
        f"operator: {operator_jwt}\n"
        "resolver: MEMORY\n"
        f"resolver_preload: {{ {account.public_key.decode()}: {account_jwt} }}\n"
    )
    creds = (
        "-----BEGIN NATS USER JWT-----\n"
        f"{user_jwt}\n"
        "------END NATS USER JWT------\n\n"
        "-----BEGIN USER NKEY SEED-----\n"
        f"{user.seed.decode()}\n"  # type: ignore[attr-defined]
        "------END USER NKEY SEED------\n"
    )
    return OperatorSetup(server_config=server_config, creds=creds)


@dataclass
class Certificates:
    ca: Path
    server_cert: Path
    server_key: Path
    client_cert: Path
    client_key: Path


def certificates(workdir: Path) -> Certificates:
    """A throwaway CA with a server certificate for 127.0.0.1/localhost and a client certificate."""
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("needs openssl on PATH")

    def run(*args: str) -> None:
        subprocess.run([openssl, *args], cwd=workdir, check=True, capture_output=True)

    # Python 3.13+ verifies with VERIFY_X509_STRICT, which needs these extensions on every certificate
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=ol-test-ca",
        "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout", "ca.key", "-out", "ca.pem")  # fmt: skip
    leaf = "authorityKeyIdentifier=keyid,issuer\nkeyUsage=critical,digitalSignature,keyEncipherment\n"
    (workdir / "server.ext").write_text(
        f"{leaf}extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n"
    )
    (workdir / "client.ext").write_text(f"{leaf}extendedKeyUsage=clientAuth\n")
    for name in ("server", "client"):
        extra = ["-extfile", f"{name}.ext"]
        run("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN=ol-test-{name}",
            "-keyout", f"{name}.key", "-out", f"{name}.csr")  # fmt: skip
        run("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
            "-days", "1", "-out", f"{name}.pem", *extra)  # fmt: skip
    return Certificates(
        ca=workdir / "ca.pem",
        server_cert=workdir / "server.pem",
        server_key=workdir / "server.key",
        client_cert=workdir / "client.pem",
        client_key=workdir / "client.key",
    )


def stream_count(url: str, stream: str) -> int:
    import nats

    async def count() -> int:
        nc = await nats.connect(url)
        info = await nc.jetstream().stream_info(stream)
        await nc.close()
        return int(info.state.messages)

    return asyncio.run(count())


def add_stream(url: str, name: str, subjects: Sequence[str]) -> None:
    import nats
    from nats.js.api import StreamConfig

    async def create() -> None:
        nc = await nats.connect(url)
        await nc.jetstream().add_stream(
            StreamConfig(name=name, subjects=list(subjects), duplicate_window=60, allow_msg_ttl=True)
        )
        await nc.close()

    asyncio.run(create())
