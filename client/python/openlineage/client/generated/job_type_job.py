# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Optional

from attr import define, field
from openlineage.client.generated.base import JobFacet


@define
class JobTypeJobFacet(JobFacet):
    processingType: str  # noqa: N815
    """Job processing type like: BATCH or STREAMING"""

    integration: str
    """OpenLineage integration type of this job: SPARK|DBT|AIRFLOW|FLINK"""

    jobType: Optional[str] = field(default=None)  # noqa: N815
    """Run type like: QUERY|COMMAND|DAG|TASK|JOB|MODEL"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
