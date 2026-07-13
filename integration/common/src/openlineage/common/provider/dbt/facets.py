# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0


import attr
from openlineage.client.facet_v2 import (
    BaseFacet,
    DatasetFacet,
    JobFacet,
    parent_run,
)
from openlineage.common.schema import GITHUB_LOCATION  # type: ignore[attr-defined]


@attr.define
class ParentRunMetadata:
    run_id: str
    job_name: str
    job_namespace: str
    root_parent_job_name: str | None = attr.field(default=None)
    root_parent_job_namespace: str | None = attr.field(default=None)
    root_parent_run_id: str | None = attr.field(default=None)

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
    project_name: str | None = attr.field(default=None)
    dbt_runtime: str | None = attr.field(default=None)
    project_version: str | None = attr.field(default=None)
    profile_name: str | None = attr.field(default=None)
    account_id: str | None = attr.field(default=None)
    # Run-wide --full-refresh flag for the invocation; None when it wasn't set.
    full_refresh: bool | None = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-run-run-facet.json"


@attr.define
class DbtPartitionBy:
    """A dbt ``partition_by`` config for an incremental model.

    Two mutually exclusive shapes, selected by adapter — a consumer should branch on
    whichever group is present rather than expect both:

    - **BigQuery** → ``field`` / ``data_type`` / ``granularity`` (object form); ``columns`` unset.
    - **Spark / other adapters** → ``columns``; the BigQuery fields unset.

    Integer-range and ingestion-time partitioning beyond these keys is not captured.

    Fields:
        field: BigQuery partition column. Example: ``"created_at"``.
        data_type: BigQuery partition type — ``date``/``timestamp``/``datetime``/``int64``.
            Example: ``"timestamp"``.
        granularity: BigQuery time grain — ``hour``/``day``/``month``/``year``. Example: ``"day"``.
        columns: Spark/other partition column(s). Example: ``["ds"]`` or ``["region", "ds"]``.
    """

    field: str | None = attr.field(default=None)
    data_type: str | None = attr.field(default=None)
    granularity: str | None = attr.field(default=None)
    columns: list[str] | None = attr.field(default=None)


@attr.define
class DbtIncrementalConfig:
    """How an incremental dbt model rebuilds data, from its resolved ``config``.

    Populated only for ``materialized == "incremental"``; its presence marks the model
    incremental even when no individual field is set.

    Which fields are populated depends on ``strategy`` — a consumer should read them by
    strategy:

    - **all strategies:** ``strategy``, ``on_schema_change``, ``full_refresh``.
    - **merge / delete+insert:** ``unique_key``, ``incremental_predicates``.
    - **microbatch:** ``event_time``, ``batch_size``, ``begin``, ``lookback``.
    - **insert_overwrite:** ``partition_by``.

    Fields:
        strategy: Incremental strategy — e.g. ``"merge"``, ``"append"``, ``"delete+insert"``,
            ``"insert_overwrite"``, ``"microbatch"``. ``None`` means the adapter default
            (dbt omits it from the manifest).
        unique_key: Key column(s) used to match existing rows (merge / delete+insert).
            Example: ``["id"]``.
        incremental_predicates: Config-declared SQL filters that bound the rows the merge
            scans (dbt ``incremental_predicates``, or the Spark ``predicates`` alias).
            Example: ``["dbt_valid_to is null"]``.
        on_schema_change: Reaction to source schema changes — ``ignore`` (default) /
            ``append_new_columns`` / ``sync_all_columns`` / ``fail``.
        event_time: (microbatch) timestamp column dbt batches on. Example: ``"event_ts"``.
        batch_size: (microbatch) batch grain — ``hour``/``day``/``month``/``year``.
        begin: (microbatch) lower bound for the first batch, ISO-8601 date or datetime.
            Example: ``"2024-01-01"``.
        lookback: (microbatch) number of prior batches to reprocess for late-arriving rows;
            integer >= 0, default ``1``. Example: ``2``.
        partition_by: (insert_overwrite) partition config; see ``DbtPartitionBy``.
        full_refresh: Per-model ``config.full_refresh`` override. The run-wide
            ``--full-refresh`` flag is a run-level concern on ``DbtRunRunFacet`` instead.
    """

    strategy: str | None = attr.field(default=None)
    unique_key: list[str] | None = attr.field(default=None)
    incremental_predicates: list[str] | None = attr.field(default=None)
    on_schema_change: str | None = attr.field(default=None)
    event_time: str | None = attr.field(default=None)
    batch_size: str | None = attr.field(default=None)
    begin: str | None = attr.field(default=None)
    lookback: int | None = attr.field(default=None)
    partition_by: DbtPartitionBy | None = attr.field(default=None)
    full_refresh: bool | None = attr.field(default=None)


@attr.define
class DbtModelConfig:
    """The resolved dbt model configuration fields most relevant for data observability.

    These come from the *resolved* ``config`` of a dbt manifest node (after applying
    project-level defaults from ``dbt_project.yml``, per-model overrides, etc.).
    """

    materialized: str | None = attr.field(default=None)
    access: str | None = attr.field(default=None)
    owner: str | None = attr.field(default=None)
    group: str | None = attr.field(default=None)
    # Set only for materialized == "incremental"; presence marks the model incremental.
    incremental: DbtIncrementalConfig | None = attr.field(default=None)


@attr.define
class DbtModelDatasetFacet(DatasetFacet):
    """Dataset facet capturing the resolved dbt ``config`` of a manifest node.

    The most observability-relevant fields are ``materialized``, ``access``, ``owner`` and
    ``group``. The free-form ``meta`` map is emitted separately as dataset/run tags (source
    ``DBT_META``), not on this facet.
    """

    config: DbtModelConfig | None = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-model-dataset-facet.json"


@attr.define
class DbtExposure:
    """A single dbt exposure that depends on a dbt model.

    Exposures are declared in dbt ``.yml`` files and describe downstream consumers of dbt
    models (dashboards, notebooks, ML applications, ...). See
    https://docs.getdbt.com/docs/build/exposures.
    """

    unique_id: str
    name: str
    type: str | None = attr.field(default=None)
    url: str | None = attr.field(default=None)


@attr.define
class DbtExposuresDatasetFacet(DatasetFacet):
    """Dataset facet listing the exposures that consume a dbt model's output dataset.

    Attached to a model's output dataset when the model builds successfully, so consumers
    can build TABLE -> EXPOSURE lineage without needing access to the manifest.
    """

    exposures: list[DbtExposure] = attr.field(factory=list)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-exposures-dataset-facet.json"


@attr.define
class DbtNodeJobFacet(JobFacet):
    """Job facet containing dbt node metadata.

    This facet embeds information from the dbt manifest schema, specifically from the node
    properties as defined in the dbt manifest specification:
    https://schemas.getdbt.com/dbt/manifest/v12/index.html#nodes_additionalProperties

    The fields in this facet correspond to properties found in dbt manifest nodes, providing
    context about the dbt model/node being executed.
    """

    original_file_path: str | None = attr.field(default=None)
    database: str | None = attr.field(default=None)
    schema: str | None = attr.field(default=None)
    alias: str | None = attr.field(default=None)
    unique_id: str | None = attr.field(default=None)
    test_type: str | None = attr.field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-node-job-facet.json"
