# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

from attr import define, field
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@define
class Job(RedactMixin):
    namespace: str
    """The namespace containing that job"""

    name: str
    """The unique name for that job within that namespace"""


@define
class ParentRunFacet(RunFacet):
    """
    the id of the parent run and job, iff this run was spawn from an other run (for example, the Dag run
    scheduling its tasks)
    """

    run: Run
    job: Job
    _additional_skip_redact: ClassVar[list[str]] = ["job", "run"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json#/$defs/ParentRunFacet"

    @classmethod
    def create(cls, runId: str, namespace: str, name: str) -> ParentRunFacet:  # noqa: N803
        import warnings

        warnings.warn(
            "ParentRunFacet.create method is deprecated. Please use class initializator instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls(run=Run(runId=runId), job=Job(namespace=namespace, name=name))


@define
class Run(RedactMixin):
    runId: str = field()  # noqa: N815
    """The globally unique ID of the run associated with the job."""

    @runId.validator
    def runid_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from uuid import UUID

        UUID(value)
