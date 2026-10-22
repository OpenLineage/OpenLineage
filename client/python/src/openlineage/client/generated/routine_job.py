# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import JobFacet


@attr.define
class RoutineJobFacet(JobFacet):
    namespace: str
    """
    Routine's host system namespace, following the OpenLineage Naming convention used for datasets in
    the same system (e.g. `bigquery`, `snowflake://my-org-myaccount`, `redshift://cluster.us-
    east-1:5439`, `postgres://host:5432`). See https://openlineage.io/docs/spec/naming/
    """
    name: str
    """
    Fully-qualified routine identifier inside the namespace, using the same path convention as tables in
    that system (e.g. `my-project.my_dataset.my_procedure` for BigQuery, `MY_DB.MY_SCHEMA.MY_PROCEDURE`
    for Snowflake)
    """
    routineType: str | None = attr.field(default=None)  # noqa: N815
    """
    Kind of routine (`PROCEDURE`, `FUNCTION`, `SCALAR_FUNCTION`, `TABLE_VALUED_FUNCTION`,
    `AGGREGATE_FUNCTION`, `WINDOW_FUNCTION`, `MACRO`, `REMOTE_FUNCTION`, etc.)
    """
    language: str | None = attr.field(default=None)
    """
    Implementation language of the routine body (`SQL`, `JAVASCRIPT`, `PYTHON`, `JAVA`, `SCALA`,
    `REMOTE`, etc.)
    """
    _additional_skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/RoutineJobFacet.json#/$defs/RoutineJobFacet"
