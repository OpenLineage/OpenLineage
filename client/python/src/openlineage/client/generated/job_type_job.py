# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import JobFacet
from openlineage.client.utils import RedactMixin


@attr.define
class EmissionPattern(RedactMixin):
    """Describes how and what the job emits in its events"""

    eventTrigger: str  # noqa: N815
    """
    Defines when events are emitted. EVENT_BASED: events emitted on lifecycle transitions
    (START/COMPLETE/FAIL/ABORT). PERIODIC: events emitted at regular time intervals.

    Example: EVENT_BASED
    """
    eventCompleteness: str  # noqa: N815
    """
    Defines what events contain. ACCUMULATIVE: Events may contain only partial information and the
    complete information can be collected by combining information from all the events emitted by a
    specific job run. COMPLETE_SNAPSHOT: events contain complete state for a specific time window
    (events can be processed independently).

    Example: ACCUMULATIVE
    """
    windowDuration: int | None = attr.field(default=None)  # noqa: N815
    """
    Time window duration for periodic event emissions in seconds. Only applicable when eventTrigger is
    PERIODIC. Required when eventTrigger is PERIODIC and eventCompleteness is COMPLETE_SNAPSHOT.

    Example: 300
    """


@attr.define
class JobTypeJobFacet(JobFacet):
    processingType: str  # noqa: N815
    """
    Job processing type: BATCH (finite jobs with clear start/end), STREAMING (continuous jobs processing
    data streams), or SERVICE (continuous long-running services). BATCH jobs are finite and emit
    START/COMPLETE/FAIL/ABORT events. STREAMING and SERVICE jobs are continuous with no natural
    completion point.

    Example: BATCH
    """
    integration: str
    """
    OpenLineage integration type of this job: for example SPARK|DBT|AIRFLOW|FLINK

    Example: SPARK
    """
    jobType: str | None = attr.field(default=None)  # noqa: N815
    """
    Run type, for example: QUERY|COMMAND|DAG|TASK|JOB|MODEL. This is an integration-specific field.

    Example: QUERY
    """
    emissionPattern: EmissionPattern | None = attr.field(default=None)  # noqa: N815
    """Describes how and what the job emits in its events"""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
