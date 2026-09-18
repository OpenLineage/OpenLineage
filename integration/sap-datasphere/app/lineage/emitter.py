# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Emit OpenLineage events via a standard OpenLineage transport.

Vendor-neutral: the transport is whatever the user configures under ``openlineage.transport`` using
OpenLineage's own transport config schema (``console``, ``http``, ``kafka``, ...). The default is
``console`` for safe local runs. Any OpenLineage-compatible backend is reached through a standard
transport (e.g. an ``http`` transport pointed at the backend's ingest URL).

Secrets in the transport config are referenced as ``${VAR}`` and resolved from the SecretsProvider
(or environment), so no keys live in committed YAML.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

from openlineage.client import OpenLineageClient
from openlineage.client.transport import get_default_factory

from app.lineage.mapping import DatasetMetadata, build_dataset_event

log = logging.getLogger(__name__)

_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expand(value, secrets):
    if isinstance(value, str):

        def repl(m: re.Match) -> str:
            key = m.group(1)
            resolved = secrets.get(key)
            if resolved is None:
                log.warning("unresolved transport placeholder", extra={"key": key})
                return ""
            return resolved

        return _PLACEHOLDER.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand(v, secrets) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v, secrets) for v in value]
    return value


class LineageEmitter:
    def __init__(self, transport_config: dict, producer: str, tenant_host: str, base_url: str | None = None) -> None:
        transport = get_default_factory().create(transport_config or {"type": "console"})
        self._client = OpenLineageClient(transport=transport)
        self._producer = producer
        self._tenant_host = tenant_host
        self._base_url = base_url
        log.info("openlineage transport initialized", extra={"transport": type(transport).__name__})

    def emit(self, meta: DatasetMetadata, event_time: datetime | None = None) -> None:
        event = build_dataset_event(
            meta,
            tenant_host=self._tenant_host,
            producer=self._producer,
            base_url=self._base_url,
            event_time=event_time,
        )
        self._client.emit(event)

    def close(self) -> None:
        closer = getattr(self._client.transport, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:  # pragma: no cover - best-effort flush
                log.debug("transport close failed", exc_info=True)


def build_emitter(settings, secrets) -> LineageEmitter:
    transport_config = _expand(dict(settings.openlineage.transport), secrets)
    return LineageEmitter(
        transport_config=transport_config,
        producer=settings.openlineage.producer,
        tenant_host=settings.datasphere.tenant_host,
        base_url=settings.datasphere.base_url,
    )
