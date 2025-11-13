# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class JobDependenciesRunFacet(RunFacet):
    """Maps execution dependencies (control flow relationships) between upstream and downstream job runs"""

    upstream: list[JobDependency] | None = attr.field(factory=list)
    """Job runs that must complete before the current run can start."""

    downstream: list[JobDependency] | None = attr.field(factory=list)
    """Job runs that will start after completion of the current run."""

    trigger_rule: str | None = attr.field(default=None)
    """
    Specifies the condition under which this job will run based on the status of upstream jobs.

    Example: ALL_SUCCESS|ALL_DONE|ONE_SUCCESS|NONE_FAILED
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json#/$defs/JobDependenciesRunFacet"


@attr.define
class JobDependency(RedactMixin):
    """Used to store all information about job dependency (e.g., job, run, etc.)."""

    job: JobIdentifier
    run: RunIdentifier | None = attr.field(default=None)
    dependency_type: str | None = attr.field(default=None)
    """
    Used to describe whether the upstream job directly triggers the downstream job, or whether the
    dependency is implicit (e.g. time-based).

    Example: DIRECT_INVOCATION|IMPLICIT_DEPENDENCY
    """
    sequence_trigger_rule: str | None = attr.field(default=None)
    """
    Used to describe the exact sequence condition on which the downstream job can be executed
    (FINISH_TO_START - downstream job can start when upstream finished; FINISH_TO_FINISH - job
    executions can overlap, but need to finish in specified order; START_TO_START - jobs need to start
    at the same time in parallel).

    Example: FINISH_TO_START|FINISH_TO_FINISH|START_TO_START
    """
    status_trigger_rule: str | None = attr.field(default=None)
    """
    Used to describe if the downstream job should be run based on the status of the upstream job.

    Example: EXECUTE_EVERY_TIME|EXECUTE_ON_SUCCESS|EXECUTE_ON_FAILURE
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json#/$defs/JobDependency"


@attr.define
class JobIdentifier(RedactMixin):
    """Used to store information about job (e.g., namespace and name)."""

    namespace: str
    """The namespace containing the job"""

    name: str
    """The unique name of a job within that namespace"""

    _skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json#/$defs/JobIdentifier"


@attr.define
class RunIdentifier(RedactMixin):
    """Used to store information about run (e.g., runId)."""

    runId: str = attr.field()  # noqa: N815
    """The globally unique ID of the run."""

    _skip_redact: ClassVar[list[str]] = ["runId"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json#/$defs/RunIdentifier"

    @runId.validator
    def runid_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from uuid import UUID

        UUID(value)
