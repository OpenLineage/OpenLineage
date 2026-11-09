# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import JobFacet
from openlineage.client.utils import RedactMixin


@attr.define
class ParentJobFacet(JobFacet):
    """
    The parent job of this job, if this job is a sub-job of another job by design (for example, a task
    defined within a DAG). Should be used on job events. It may also be used on run events when the
    specific run of the parent job is not known; if a ParentRunFacet is also present, it takes
    precedence.
    """

    job: ParentJobFacetJob
    root: ParentJobFacetRoot | None = attr.field(default=None)
    _additional_skip_redact: ClassVar[list[str]] = ["job"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json#/$defs/ParentJobFacet"


@attr.define
class ParentJobFacetJob(RedactMixin):
    namespace: str
    """
    The namespace containing that job

    Example: my-scheduler-namespace
    """
    name: str
    """
    The unique name for that job within that namespace

    Example: myjob
    """
    facets: dict[str, JobFacet] | None = attr.field(factory=dict)
    """
    Selected subset of facets of the referenced job (not the current job), forwarded here for
    convenience. See the referenced job's own event for full information.
    """
    _skip_redact: ClassVar[list[str]] = ["namespace", "name"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json#/$defs/ParentJobFacetJob"


@attr.define
class ParentJobFacetRoot(RedactMixin):
    job: ParentJobFacetJob
    _skip_redact: ClassVar[list[str]] = ["job"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/ParentJobFacet.json#/$defs/ParentJobFacetRoot"
