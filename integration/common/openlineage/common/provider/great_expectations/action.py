# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from openlineage.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    JobFacet,
    RunFacet,
    data_quality_metrics_input_dataset,
    documentation_job,
    parent_run,
    source_code_location_job,
)
from openlineage.client.serde import Serde
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.dataset import Dataset as OLDataset
from openlineage.common.provider.great_expectations.facets import (
    GreatExpectationsAssertionsDatasetFacet,
    GreatExpectationsRunFacet,
)
from openlineage.common.provider.great_expectations.results import (
    COLUMN_EXPECTATIONS_PARSER,
    EXPECTATIONS_PARSERS,
    GreatExpectationsAssertion,
)
from openlineage.common.provider.snowflake import fix_snowflake_sqlalchemy_uri
from openlineage.common.sql import parse

from great_expectations.checkpoint import ValidationAction
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.dataset import Dataset as GEDataset
from great_expectations.dataset import PandasDataset, SqlAlchemyDataset
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.validator.validator import Validator

# There is no guarantee that SqlAlchemy is available with Great Expectations.
# Especially, it could be used only with Pandas datasets, in which case
# we shouldn't try to import it.
# Great Expectations itself tries hard to not import SqlAlchemy if not needed.
try:
    from sqlalchemy import MetaData, Table
    from sqlalchemy.engine import Connection
except ImportError:
    MetaData = None  # type: ignore
    Table = None  # type: ignore
    Connection = None  # type: ignore


class OpenLineageValidationAction(ValidationAction):
    """
    ValidationAction implementation which posts RunEvents for a GreatExpectations validation job.

    Openlineage host parameters can be passed in as constructor arguments or environment variables
    will be searched. Job information can optionally be passed in as constructor arguments or the
    great expectations suite name and batch identifier will be used as the job name
    (the namespace should be passed in as either a constructor arg or as an environment variable).

    The data_asset will be inspected to determine the dataset source- SqlAlchemy datasets and
    Pandas datasets are supported. SqlAlchemy datasets are typically treated as other SQL data
    sources in OpenLineage. The database host and database name are treated as the data "source"
    and the schema + table are treated as the table name. Columns are fetched when possible and the
    schema will be posted as a facet. Some special handling for Bigquery is included, as "bigquery"
    is always the data source, while the table name consists of "project.dataset.table".

    Both the GreatExpectationsAssertionsDatasetFacet and DataQualityDatasetFacet are attached to
    *each* dataset found in the data_asset (this includes tables that are joined in a `custom_sql`
    argument). The DataQualityDatasetFacet is also posted as the more standard OpenLineage
    DataQualityMetricsInputDatasetFacet.

    The resulting RunEvent is returned from the _run method, so it can be seen in the
    actions_results field of the validation results.
    """

    def __init__(
        self,
        data_context,
        openlineage_host=None,
        openlineage_namespace=None,
        openlineage_apiKey=None,
        openlineage_parent_run_id=None,
        openlineage_parent_job_namespace=None,
        openlineage_parent_job_name=None,
        openlineage_root_parent_run_id=None,
        openlineage_root_parent_job_namespace=None,
        openlineage_root_parent_job_name=None,
        job_name=None,
        job_description=None,
        code_location=None,
        openlineage_run_id=None,
        do_publish=True,
    ):
        super().__init__(data_context)
        if openlineage_host is not None:
            self.openlineage_client = OpenLineageClient(
                openlineage_host, OpenLineageClientOptions(api_key=openlineage_apiKey)
            )
        else:
            self.openlineage_client = OpenLineageClient.from_environment()
        if openlineage_namespace is not None:
            self.namespace = openlineage_namespace
        else:
            self.namespace = os.getenv("OPENLINEAGE_NAMESPACE", "default")
        if openlineage_run_id is not None:
            self.run_id = openlineage_run_id
        else:
            self.run_id = generate_new_uuid()
        self.parent_run_id = openlineage_parent_run_id
        self.parent_job_namespace = openlineage_parent_job_namespace
        self.parent_job_name = openlineage_parent_job_name
        self.root_parent_run_id = openlineage_root_parent_run_id
        self.root_parent_job_namespace = openlineage_root_parent_job_namespace
        self.root_parent_job_name = openlineage_root_parent_job_name
        self.job_name = job_name
        self.job_description = job_description
        self.code_location = code_location
        self.do_publish = do_publish

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        data_asset: Union[GEDataset, Validator],
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        payload=None,
    ):
        # Initialize logger here so that the action is serializable until it actually runs
        self.log = logging.getLogger(self.__class__.__module__ + "." + self.__class__.__name__)

        datasets = []
        if isinstance(data_asset, SqlAlchemyDataset):
            datasets = self._fetch_datasets_from_sql_source(data_asset, validation_result_suite)
        elif isinstance(data_asset, PandasDataset):
            datasets = self._fetch_datasets_from_pandas_source(data_asset, validation_result_suite)
        elif isinstance(data_asset.execution_engine, SqlAlchemyExecutionEngine):
            datasets = self._fetch_datasets_from_sql_source(data_asset, validation_result_suite)
        elif isinstance(data_asset.execution_engine, PandasExecutionEngine):
            datasets = self._fetch_datasets_from_pandas_source(data_asset, validation_result_suite)
        run_facets: Dict[str, RunFacet] = {}
        if self.parent_run_id is not None:
            root = None
            if self.root_parent_run_id is not None:
                root = parent_run.Root(
                    run=parent_run.RootRun(self.root_parent_run_id),
                    job=parent_run.RootJob(
                        namespace=self.root_parent_job_namespace, name=self.root_parent_job_name
                    ),
                )
            run_facets.update(
                {
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId=self.parent_run_id),
                        job=parent_run.Job(namespace=self.parent_job_namespace, name=self.parent_job_name),
                        root=root,
                    ),
                }
            )

        # workaround for GE v2 and v3 API difference
        suite_meta = dict(
            {key: self._ser(value) for key, value in copy.deepcopy(validation_result_suite.meta).items()}
        )
        if "expectation_suite_meta" not in suite_meta:
            suite_meta["expectation_suite_meta"] = dict(
                {key: self._ser(value) for key, value in copy.deepcopy(validation_result_suite.meta).items()}
            )
        run_facets.update(
            {
                "great_expectations_meta": GreatExpectationsRunFacet(
                    **suite_meta,
                )
            }
        )
        job_facets: Dict[str, JobFacet] = {}
        if self.job_description:
            job_facets["documentation"] = documentation_job.DocumentationJobFacet(self.job_description)
        if self.code_location:
            job_facets["sourceCodeLocation"] = source_code_location_job.SourceCodeLocationJobFacet(
                type="", url=self.code_location
            )

        job_name = self.job_name
        if self.job_name is None:
            job_name = (
                validation_result_suite.meta["expectation_suite_name"]
                + "."
                + validation_result_suite_identifier.batch_identifier
            )
        run_event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat(),
            run=Run(runId=str(self.run_id), facets=run_facets),
            job=Job(self.namespace, job_name, facets=job_facets),
            inputs=datasets,
            outputs=[],
            producer="https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/common/openlineage/provider/great_expectations",
        )
        if self.do_publish:
            self.openlineage_client.emit(run_event)
        # Great expectations tries to append stuff here, so we need to make it a dict
        return Serde.to_dict(run_event)

    def _ser(self, obj):
        if hasattr(obj, "to_json_dict"):
            return obj.to_json_dict()
        else:
            return obj

    def _fetch_datasets_from_pandas_source(
        self,
        data_asset: Union[PandasDataset, Validator],
        validation_result_suite: ExpectationSuiteValidationResult,
    ) -> List[OLDataset]:
        """
        Generate a list of OpenLineage Datasets from a PandasDataset
        :param data_asset:
        :param validation_result_suite:
        :return:
        """
        if isinstance(data_asset, PandasDataset):
            if data_asset.batch_kwargs.__contains__("path"):
                path = data_asset.batch_kwargs.get("path")
                if path.startswith("/"):
                    path = f"file://{path}"
                parsed_url = urlparse(path)
                columns = [
                    Field(
                        name=col,
                        type=str(data_asset[col].dtype) if data_asset[col].dtype is not None else "UNKNOWN",
                    )
                    for col in data_asset.columns
                ]
                return [
                    Dataset(
                        source=self._source(parsed_url._replace(path="")),
                        name=parsed_url.path,
                        fields=columns,
                        input_facets=self.results_facet(validation_result_suite),
                    ).to_openlineage_dataset()
                ]
            return []
        else:
            batch = data_asset.active_batch
            batch_data = batch.data
            path = (
                batch.batch_request.runtime_parameters.get("path", None)
                if batch.batch_request.runtime_parameters is not None
                else None
            )
            if path is None:
                return []
            if path.startswith("/"):
                path = f"file://{path}"
            parsed_url = urlparse(path)
            columns = [
                Field(
                    name=col,
                    type=str(data_asset[col].dtype) if data_asset[col].dtype is not None else "UNKNOWN",
                )
                for col in batch_data.columns
            ]
            return [
                Dataset(
                    source=self._source(parsed_url._replace(path="")),
                    name=parsed_url.path,
                    fields=columns,
                    input_facets=self.results_facet(validation_result_suite),
                ).to_openlineage_dataset()
            ]

    def _fetch_datasets_from_sql_source(
        self,
        data_asset: Union[SqlAlchemyDataset, Validator],
        validation_result_suite: ExpectationSuiteValidationResult,
    ) -> List[OLDataset]:
        """
        Generate a list of OpenLineage Datasets from a SqlAlchemyDataset.
        :param data_asset:
        :param validation_result_suite:
        :return:
        """
        metadata = MetaData()
        if isinstance(data_asset, SqlAlchemyDataset):
            if data_asset.generated_table_name is not None:
                custom_sql = data_asset.batch_kwargs.get("query")
                parsed_sql = parse(custom_sql, dialect=data_asset.engine.dialect.name.lower())
                return [
                    self._get_sql_table(data_asset, metadata, t.schema, t.name, validation_result_suite)
                    for t in parsed_sql.in_tables
                ]
            return [
                self._get_sql_table(
                    data_asset,
                    metadata,
                    data_asset._table.schema,
                    data_asset._table.name,
                    validation_result_suite,
                )
            ]
        else:
            batch = data_asset.active_batch
            batch_data = batch["data"]
            custom_sql = (
                batch.batch_request.runtime_parameters.get("query", None)
                if batch.batch_request.runtime_parameters is not None
                else None
            )
            if custom_sql:
                parsed_sql = parse(custom_sql, dialect=data_asset.execution_engine.dialect_name)
                return [
                    self._get_sql_table(batch_data, metadata, t.schema, t.name, validation_result_suite)
                    for t in parsed_sql.in_tables
                ]
            table_name = batch["batch_spec"]["table_name"]
            try:
                schema_name = batch["batch_spec"]["schema_name"]
            except KeyError:
                schema_name = None
            return [
                self._get_sql_table(
                    batch_data,
                    metadata,
                    schema_name,
                    table_name,
                    validation_result_suite,
                )
            ]

    def _get_sql_table(
        self,
        data_asset: Union[SqlAlchemyDataset, SqlAlchemyBatchData],
        meta: MetaData,
        schema: Optional[str],
        table_name: str,
        validation_result_suite: ExpectationSuiteValidationResult,
    ) -> Optional[OLDataset]:
        """
        Construct a Dataset from the connection url and the columns returned from the
        SqlAlchemyDataset
        :param data_asset:
        :return:
        """
        engine = data_asset.engine if isinstance(data_asset, SqlAlchemyDataset) else data_asset._engine
        if isinstance(engine, Connection):
            engine = engine.engine
        datasource_url = engine.url

        if engine.dialect.name.lower() == "snowflake":
            if engine.connection_string:
                datasource_url = engine.connection_string
            else:
                datasource_url = engine.url
            datasource_url = fix_snowflake_sqlalchemy_uri(datasource_url)

        # bug in sql parser doesn't strip ` character from bigquery tables
        if table_name.endswith("`") or table_name.startswith("`"):
            table_name = table_name.replace("`", "")
        if engine.dialect.name.lower() == "bigquery":
            schema = f"{datasource_url.host}.{datasource_url.database}"

        table = Table(table_name, meta, autoload_with=engine)

        fields = [
            Field(
                name=key,
                type=str(col.type) if col.type is not None else "UNKNOWN",
                description=col.doc,
            )
            for key, col in table.columns.items()
        ]

        name = table_name if schema is None else f"{schema}.{table_name}"

        results_facet = self.results_facet(validation_result_suite)
        return Dataset(
            source=self._source(urlparse(str(datasource_url))),
            fields=fields,
            name=name,
            input_facets=results_facet,
        ).to_openlineage_dataset()

    def _source(self, url) -> Source:
        """
        Construct a Source from the connection url. Special handling for BigQuery is included.
        We attempt to strip credentials from the connection url, if present.
        :param url: a parsed url, as returned from urlparse()
        :return:
        """

        if url.scheme == "bigquery":
            return Source(scheme="bigquery", connection_url="bigquery")

        return Source(
            scheme=url.scheme,
            authority=url.hostname,
            # Remove credentials from the URL if present
            connection_url=url._replace(netloc=url.hostname, query=None, fragment=None).geturl(),
        )

    def results_facet(self, validation_result: ExpectationSuiteValidationResult):
        """
        Parse the validation result and extract input facets based on the results. We'll return a
        DataQualityDatasetFacet, a GreatExpectationsAssertionsDatasetFacet, and a
        (openlineage standard) DataQualityMetricsInputDatasetFacet
        :param validation_result:
        :return:
        """
        try:
            data_quality_facet = self.parse_data_quality_facet(validation_result)
            if not data_quality_facet:
                return None

            assertions_facet = self.parse_assertions(validation_result)
            if not assertions_facet:
                return None
            return {
                "dataQuality": data_quality_facet,
                "greatExpectations_assertions": assertions_facet,
                "dataQualityMetrics": data_quality_facet,
            }

        except ValueError:
            self.log.exception("Exception while retrieving great expectations dataset")
        return None

    def parse_data_quality_facet(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> Optional[data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet]:
        """
        Parse the validation result and extract a DataQualityDatasetFacet
        :param validation_result:
        :return:
        """
        facet_data: Dict[str, defaultdict] = {"columnMetrics": defaultdict(dict)}

        # try to get to actual expectations results
        try:
            expectations_results = validation_result["results"]
            for expectation in expectations_results:
                for parser in EXPECTATIONS_PARSERS:
                    # accept possible duplication, should have no difference in results
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data[result.facet_key] = result.value
                for parser in COLUMN_EXPECTATIONS_PARSER:
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data["columnMetrics"][result.column_id][result.facet_key] = result.value

            for key in facet_data["columnMetrics"].keys():
                facet_data["columnMetrics"][key] = data_quality_metrics_input_dataset.ColumnMetrics(
                    **facet_data["columnMetrics"][key]
                )
            return data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet(**facet_data)  # type: ignore[arg-type]
        except ValueError:
            self.log.exception("Great Expectations's CheckpointResult object does not have expected key")
        return None

    def parse_assertions(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> Optional[GreatExpectationsAssertionsDatasetFacet]:
        assertions = []

        try:
            for expectation in validation_result.results:
                assertions.append(
                    GreatExpectationsAssertion(
                        expectationType=expectation["expectation_config"]["expectation_type"],
                        success=expectation["success"],
                        column=expectation["expectation_config"]["kwargs"].get("column", None),
                    )
                )

            return GreatExpectationsAssertionsDatasetFacet(assertions)
        except ValueError:
            self.log.exception("Great Expectations's CheckpointResult object does not have expected key")
        return None
