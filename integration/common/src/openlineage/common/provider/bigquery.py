# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import logging
import traceback
import warnings
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import attr
from openlineage.client.facet_v2 import BaseFacet, external_query_run, output_statistics_output_dataset
from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.schema import GITHUB_LOCATION  # type: ignore[attr-defined]
from openlineage.common.sql import DbTableMeta
from openlineage.common.utils import get_from_nullable_chain

_BIGQUERY_CONN_URL = "bigquery"

# we lazy-load bigquery Client in BigQueryDatasetsProvider if not type-checking
if TYPE_CHECKING:
    from google.cloud.bigquery import Client


def get_bq_client():
    from google.cloud.bigquery import Client

    return Client


@attr.define
class BigQueryErrorRunFacet(BaseFacet):
    """
    Represents errors that can happen during execution of BigqueryExtractor
    :param clientError: represents errors originating in bigquery client
    :param parserError: represents errors that happened during parsing SQL provided to bigquery
    """

    clientError: Optional[str] = None
    parserError: Optional[str] = None

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "bq-error-run-facet.json"


@attr.define
class BigQueryJobRunFacet(BaseFacet):
    """
    Facet that represents relevant statistics of bigquery run.
    :param cached: bigquery caches query results. Rest of the statistics will not be provided
        for cached queries.
    :param billedBytes: how many bytes bigquery bills for.
    :param properties: full property tree of bigquery run.
    """

    cached: bool
    billedBytes: Optional[int] = None
    properties: Optional[str] = None

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "bq-statistics-run-facet.json"


@attr.define
class BigQueryStatisticsDatasetFacet(BaseFacet):
    """
    Facet that represents statistics of output dataset resulting from bigquery run.
    :param outputRows: how many rows query produced.
    :param size: size of output dataset in bytes.
    """

    rowCount: int
    size: int

    def to_openlineage(self) -> output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet:
        return output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
            rowCount=self.rowCount, size=self.size
        )

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "bq-statistics-dataset-facet.json"


@attr.define
class BigQueryFacets:
    run_facets: Dict[str, BaseFacet]
    inputs: List[Dataset]
    outputs: List[Dataset]
    _output: Optional[Dataset] = None

    @property
    def output(self) -> Optional[Dataset]:
        warnings.warn(
            "The 'output' attribute will be removed in a future version. Use 'outputs' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._output

    @output.setter
    def output(self, value: Optional[Dataset]) -> None:
        self._output = value


class BigQueryDatasetsProvider:
    def __init__(self, client: Optional["Client"] = None, logger: Optional[logging.Logger] = None):
        if client is None:
            # lazy-load bigquery client since its slow to import (primarily due to pandas)
            self.client = get_bq_client()
        else:
            self.client = client
        if logger is None:
            self.logger: logging.Logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def get_facets(self, job_id: str) -> BigQueryFacets:
        inputs = []
        outputs = []
        run_facets: Dict[str, BaseFacet] = {
            "externalQuery": external_query_run.ExternalQueryRunFacet(
                externalQueryId=job_id, source="bigquery"
            )
        }
        self.logger.debug("Extracting data from bigquery job: `%s`", job_id)
        try:
            try:
                job = self.client.get_job(job_id=job_id)  # type: ignore
                props = job._properties

                if get_from_nullable_chain(props, ["status", "state"]) != "DONE":
                    raise ValueError(f"Trying to extract data from running bigquery job: `{job_id}`")

                run_facets["bigQuery_job"] = self._get_job_run_facet(props)

                if get_from_nullable_chain(props, ["statistics", "numChildJobs"]):
                    self.logger.debug("Found SCRIPT job. Extracting lineage from child jobs instead.")
                    # SCRIPT job type has no input / output information but spawns child jobs that have one
                    # https://cloud.google.com/bigquery/docs/information-schema-jobs#multi-statement_query_job
                    for child_job_id in self.client.list_jobs(parent_job=job_id):
                        child_job = self.client.get_job(job_id=child_job_id)  # type: ignore
                        child_inputs, child_output = self._get_input_output(child_job._properties)
                        inputs.extend(child_inputs)
                        if child_output:
                            outputs.append(child_output)
                else:
                    inputs, _output = self._get_input_output(props)
                    if _output:
                        outputs.append(_output)
            finally:
                # Ensure client has close() defined, otherwise ignore.
                # NOTE: close() was introduced in python-bigquery v1.23.0
                if hasattr(self.client, "close"):
                    self.client.close()
        except Exception as e:
            self.logger.error(f"Cannot retrieve job details from BigQuery.Client. {e}", exc_info=True)
            run_facets.update(
                {
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {traceback.format_exc()}",
                    )
                }
            )
        outputs = self._deduplicate_outputs(outputs)
        # For complex scripts there can be multiple outputs - in that case keep them all in `outputs` and
        # leave the `output` empty to avoid providing misleading information. When the script has a single
        # output (f.e. a single statement with some variable declarations), treat it as a regular non-script
        # job and put the output in `output` as an addition to new `outputs`. `output` is deprecated.
        return BigQueryFacets(
            run_facets=run_facets,
            inputs=list({i.name: i for i in inputs if i}.values()),
            outputs=outputs,
            output=outputs[0] if len(outputs) == 1 else None,
        )

    @staticmethod
    def _deduplicate_outputs(outputs) -> List[Dataset]:
        # Sources are the same so we can compare only names
        final_outputs = {}
        for single_output in outputs:
            if not single_output:
                continue
            key = single_output.name
            if key not in final_outputs:
                final_outputs[key] = single_output
                continue

            # No BigQueryStatisticsDatasetFacet is added to duplicated outputs as we can not determine
            # if the rowCount or size can be summed together.
            single_output.custom_facets.pop("stats", None)
            single_output.output_facets.pop("outputStatistics", None)
            final_outputs[key] = single_output

        return list(final_outputs.values())

    def _get_input_output(self, properties) -> Tuple[List[Dataset], Optional[Dataset]]:
        dataset_stat_facet = self._get_statistics_dataset_facet(properties)
        inputs = self._get_input_from_bq(properties)
        output = self._get_output_from_bq(properties)

        if output and dataset_stat_facet:
            output.custom_facets.update({"stats": dataset_stat_facet})
            output.output_facets.update({"outputStatistics": dataset_stat_facet.to_openlineage()})

        return inputs, output

    @staticmethod
    def _get_job_run_facet(properties) -> BigQueryJobRunFacet:
        if get_from_nullable_chain(properties, ["configuration", "query", "query"]):
            # Exclude the query to avoid event size issues and duplicating SqlJobFacet information.
            properties = copy.deepcopy(properties)
            properties["configuration"]["query"].pop("query")
        cache_hit = get_from_nullable_chain(properties, ["statistics", "query", "cacheHit"])
        billed_bytes = get_from_nullable_chain(properties, ["statistics", "query", "totalBytesBilled"])
        return BigQueryJobRunFacet(
            cached=str(cache_hit).lower() == "true",
            billedBytes=int(billed_bytes) if billed_bytes else None,
            properties=json.dumps(properties),
        )

    @staticmethod
    def _get_statistics_dataset_facet(properties) -> Optional[BigQueryStatisticsDatasetFacet]:
        query_plan = get_from_nullable_chain(properties, ["statistics", "query", "queryPlan"])
        if not query_plan:
            return None

        out_stage = query_plan[-1]
        out_rows = out_stage.get("recordsWritten", None)
        out_bytes = out_stage.get("shuffleOutputBytes", None)
        if out_bytes and out_rows:
            return BigQueryStatisticsDatasetFacet(rowCount=int(out_rows), size=int(out_bytes))
        return None

    def _get_input_from_bq(self, properties):
        bq_input_tables = get_from_nullable_chain(properties, ["statistics", "query", "referencedTables"])
        if not bq_input_tables:
            return []

        input_table_names = [self._bq_table_name(bq_t) for bq_t in bq_input_tables]
        sources = [self._source() for bq_t in bq_input_tables]
        try:
            return [
                Dataset.from_table_schema(source=source, table_schema=table_schema)
                for table_schema, source in zip(self._get_table_schemas(input_table_names), sources)
            ]
        except Exception as e:
            self.logger.warning(f"Could not extract schema from bigquery. {e}")
            return [Dataset.from_table(source, table) for table, source in zip(input_table_names, sources)]

    def _get_output_from_bq(self, properties) -> Optional[Dataset]:
        bq_output_table = get_from_nullable_chain(properties, ["configuration", "query", "destinationTable"])
        if not bq_output_table:
            return None

        output_table_name = self._bq_table_name(bq_output_table)
        source = self._source()

        table_schema = self._get_table_safely(output_table_name)
        if table_schema:
            return Dataset.from_table_schema(
                source=source,
                table_schema=table_schema,
            )
        else:
            self.logger.warning("Could not resolve output table from bq")
            return Dataset.from_table(source, output_table_name)

    def _get_table_safely(self, output_table_name):
        try:
            return self._get_table(output_table_name)
        except Exception as e:
            self.logger.warning(f"Could not extract output schema from bigquery. {e}")
        return None

    def _get_table_schemas(self, tables: List[str]) -> List[DbTableSchema]:
        # Avoid querying BigQuery by returning an empty array
        # if no tables have been provided.
        if not tables:
            return []

        return [schema for table in tables if (schema := self._get_table(table)) is not None]

    def _get_table(self, table: str) -> Optional[DbTableSchema]:
        bq_table = self.client.get_table(table)
        if not bq_table._properties:
            return None
        table_prop = bq_table._properties

        fields = get_from_nullable_chain(table_prop, ["schema", "fields"])
        if not fields:
            return None

        columns = [
            DbColumn(
                name=fields[i].get("name"),
                type=fields[i].get("type"),
                description=fields[i].get("description"),
                ordinal_position=i,
            )
            for i in range(len(fields))
        ]

        return DbTableSchema(
            schema_name=table_prop.get("tableReference").get("projectId")
            + "."
            + table_prop.get("tableReference").get("datasetId"),
            table_name=DbTableMeta(table_prop.get("tableReference").get("tableId")),
            columns=columns,
        )

    def _source(self) -> Source:
        return Source(scheme="bigquery", connection_url="bigquery")

    def _bq_table_name(self, bq_table):
        project = bq_table.get("projectId")
        dataset = bq_table.get("datasetId")
        table = bq_table.get("tableId")
        return f"{project}.{dataset}.{table}"
