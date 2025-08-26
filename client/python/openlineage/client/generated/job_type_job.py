# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from attr import define
from attr import field as attr_field
from openlineage.client.generated.base import JobFacet


@define
class JobTypeJobFacet(JobFacet):
    processingType: str  # noqa: N815
    """Job processing type like: BATCH or STREAMING"""

    integration: str
    """OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK"""

    jobType: str | None = attr_field(default=None)  # noqa: N815
    """Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific field."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
