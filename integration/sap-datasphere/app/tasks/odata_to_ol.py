# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Task: scan SAP Datasphere over OData and emit an OpenLineage DatasetEvent per dataset.

The whole task is idempotent -- it emits a fresh snapshot each run. Per-dataset and per-operation
errors are captured (never abort the run) and surfaced on the dataset event via ``ODataErrorFacet``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.config import Settings
from app.datasphere.client import build_client
from app.datasphere.errors import ODataRequestError
from app.datasphere.models import Dataset, DatasetSchema, Space
from app.lastupdated import build_resolvers
from app.lineage.emitter import build_emitter
from app.lineage.mapping import DatasetMetadata

log = logging.getLogger(__name__)


def resolve_enabled_spaces(spaces: list[Space], spaces_config) -> list[Space]:
    include = set(spaces_config.include)
    exclude = set(spaces_config.exclude)
    result = [s for s in spaces if (not include or s.id in include) and s.id not in exclude]
    return result


def collect_metadata(client, resolvers, dataset: Dataset, space_label: str | None = None) -> DatasetMetadata:
    errors = []

    schema: DatasetSchema | None = None
    try:
        schema = client.get_schema(dataset)
    except ODataRequestError as exc:
        errors.append(exc.as_error("schema"))

    # rowCount comes from the relational OData entity's $count. Analytical-only assets have no
    # relational endpoint (and analytical $count returns 404), so skip rather than record an error.
    row_count = None
    if dataset.relational_data_url:
        try:
            row_count = client.get_row_count(dataset)
        except ODataRequestError as exc:
            errors.append(exc.as_error("row_count"))

    last_updated = None
    for resolver in resolvers:
        try:
            result = resolver.resolve(client, dataset, schema or DatasetSchema())
        except ODataRequestError as exc:
            errors.append(exc.as_error(f"last_updated:{resolver.name}"))
            continue
        if result.found:
            last_updated = result
            break

    return DatasetMetadata(
        dataset=dataset,
        schema=schema,
        row_count=row_count,
        last_updated=last_updated,
        space_label=space_label,
        errors=errors,
    )


def run_odata_to_ol(settings: Settings, secrets) -> dict:
    client = build_client(settings, secrets)
    emitter = build_emitter(settings, secrets)
    resolvers = build_resolvers(settings.lastupdated)
    event_time = datetime.now(timezone.utc)

    summary = {"spaces": 0, "datasets": 0, "emitted": 0, "with_errors": 0}
    try:
        spaces = resolve_enabled_spaces(client.list_spaces(), settings.spaces)
        summary["spaces"] = len(spaces)
        log.info("scan started", extra={"spaces": [s.id for s in spaces]})

        for space in spaces:
            try:
                datasets = client.list_datasets(space.id)
            except ODataRequestError as exc:
                log.error(
                    "failed to list datasets",
                    extra={"space": space.id, "status": exc.http_status, "error": exc.message},
                )
                continue

            log.info("space enumerated", extra={"space": space.id, "datasets": len(datasets)})
            for dataset in datasets:
                summary["datasets"] += 1
                meta = collect_metadata(client, resolvers, dataset, space_label=space.name)
                emitter.emit(meta, event_time=event_time)
                summary["emitted"] += 1
                if meta.errors:
                    summary["with_errors"] += 1
                log.debug(
                    "emitted dataset",
                    extra={
                        "space": space.id,
                        "asset": dataset.asset_id,
                        "row_count": meta.row_count,
                        "last_updated_source": meta.last_updated.source if meta.last_updated else None,
                        "errors": len(meta.errors),
                    },
                )
        log.info("scan complete", extra=summary)
        return summary
    finally:
        emitter.close()
        client.close()
