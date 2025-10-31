# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class JobDependenciesRunFacet(RunFacet):
    """direct upstream and downstream relationships between job runs"""

    upstream: list[JobDependency] | None = attr.field(factory=list)
    """Job runs that must complete before the current run can start."""

    downstream: list[JobDependency] | None = attr.field(factory=list)
    """Job runs that are waiting for the successful completion of the current run."""

    trigger_rule: str | None = attr.field(default=None)
    """
    Specifies the condition under which this job will run based on the status of upstream jobs, for
    example: ALL_SUCCESS|ALL_DONE|ONE_SUCCESS|NONE_FAILED.
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json#/$defs/JobDependenciesRunFacet"


@attr.define
class JobDependency(RedactMixin):
    """Used to store all information about job dependency (e.g., job, run, root etc.)."""

    job: JobIdentifier
    run: RunIdentifier | None = attr.field(default=None)
    root: RootIdentifier | None = attr.field(default=None)

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
class RootIdentifier(RedactMixin):
    """Used to store information about root job and root run."""

    run: RunIdentifier
    job: JobIdentifier

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/JobDependenciesRunFacet.json#/$defs/RootIdentifier"


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
