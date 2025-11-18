# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

import attr
from openlineage.client.facet_v2 import (
    BaseFacet,
    JobFacet,
    parent_run,
)
from openlineage.common.schema import GITHUB_LOCATION  # type: ignore[attr-defined]


@attr.define
class ParentRunMetadata:
    run_id: str
    job_name: str
    job_namespace: str
    root_parent_job_name: Optional[str] = attr.field(default=None)
    root_parent_job_namespace: Optional[str] = attr.field(default=None)
    root_parent_run_id: Optional[str] = attr.field(default=None)

    def to_openlineage(self) -> parent_run.ParentRunFacet:
        root = None
        if self.root_parent_run_id and self.root_parent_job_namespace and self.root_parent_job_name:
            root = parent_run.Root(
                run=parent_run.RootRun(runId=self.root_parent_run_id),
                job=parent_run.RootJob(
                    namespace=self.root_parent_job_namespace, name=self.root_parent_job_name
                ),
            )

        return parent_run.ParentRunFacet(
            run=parent_run.Run(runId=self.run_id),
            job=parent_run.Job(namespace=self.job_namespace, name=self.job_name),
            root=root,
        )


@attr.define
class DbtVersionRunFacet(BaseFacet):
    version: str

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-version-run-facet.json"


@attr.define
class DbtRunRunFacet(BaseFacet):
    invocation_id: str
    project_name: Optional[str] = attr.field(default=None)
    dbt_runtime: Optional[str] = attr.field(default=None)
    project_version: Optional[str] = attr.field(default=None)
    profile_name: Optional[str] = attr.field(default=None)
    account_id: Optional[str] = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-run-run-facet.json"


@attr.define
class DbtNodeJobFacet(JobFacet):
    """Job facet containing dbt node metadata.

    This facet embeds information from the dbt manifest schema, specifically from the node
    properties as defined in the dbt manifest specification:
    https://schemas.getdbt.com/dbt/manifest/v12/index.html#nodes_additionalProperties

    The fields in this facet correspond to properties found in dbt manifest nodes, providing
    context about the dbt model/node being executed.
    """

    original_file_path: Optional[str] = attr.field(default=None)
    database: Optional[str] = attr.field(default=None)
    schema: Optional[str] = attr.field(default=None)
    alias: Optional[str] = attr.field(default=None)
    unique_id: Optional[str] = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-node-job-facet.json"
