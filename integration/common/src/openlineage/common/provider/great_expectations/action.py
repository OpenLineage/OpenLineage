# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Literal, Optional
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
from great_expectations.compatibility.pydantic import validator
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
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


log = logging.getLogger(__name__)


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

    # Pydantic field definitions with defaults
    type: Literal["openlineage"] = "openlineage"
    openlineage_host: Optional[str] = None
    openlineage_namespace: Optional[str] = None
    openlineage_apiKey: Optional[str] = None
    openlineage_parent_run_id: Optional[str] = None
    openlineage_parent_job_namespace: Optional[str] = None
    openlineage_parent_job_name: Optional[str] = None
    openlineage_root_parent_run_id: Optional[str] = None
    openlineage_root_parent_job_namespace: Optional[str] = None
    openlineage_root_parent_job_name: Optional[str] = None
    job_name: Optional[str] = None
    job_description: Optional[str] = None
    code_location: Optional[str] = None
    openlineage_run_id: Optional[str] = None
    do_publish: bool = True

    @validator("openlineage_namespace", pre=True, always=True)
    def set_namespace(cls, v):
        return v or os.getenv("OPENLINEAGE_NAMESPACE", "default")

    @validator("openlineage_run_id", pre=True, always=True)
    def set_run_id(cls, v):
        return v or str(generate_new_uuid())

    def __init__(self, data_context=None, **data):
        # data_context is required by ValidationAction in some GE versions
        # but we don't use it directly
        super().__init__(data_context=data_context, **data)

        # Initialize OpenLineage client
        if self.openlineage_host is not None:
            object.__setattr__(
                self,
                "openlineage_client",
                OpenLineageClient(
                    self.openlineage_host, OpenLineageClientOptions(api_key=self.openlineage_apiKey)
                ),
            )
        else:
            object.__setattr__(self, "openlineage_client", OpenLineageClient.from_environment())

    def _run(
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
        data_asset: Validator,  # Modern GE v3 uses Validator only
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
        payload=None,
    ):
        datasets = []
        # Modern GE v3 API: all data assets are Validators with execution engines
        if isinstance(data_asset.execution_engine, SqlAlchemyExecutionEngine):
            datasets = self._fetch_datasets_from_sql_source(data_asset, validation_result_suite)
        elif isinstance(data_asset.execution_engine, PandasExecutionEngine):
            datasets = self._fetch_datasets_from_pandas_source(data_asset, validation_result_suite)
        else:
            log.warning(f"Unsupported execution engine type: {type(data_asset.execution_engine)}")
            datasets = []
        run_facets: Dict[str, RunFacet] = {}
        if self.openlineage_parent_run_id is not None:
            root = None
            if self.openlineage_root_parent_run_id is not None:
                root = parent_run.Root(
                    run=parent_run.RootRun(self.openlineage_root_parent_run_id),
                    job=parent_run.RootJob(
                        namespace=self.openlineage_root_parent_job_namespace,  # type: ignore [arg-type]
                        name=self.openlineage_root_parent_job_name,  # type: ignore [arg-type]
                    ),
                )
            run_facets.update(
                {
                    "parent": parent_run.ParentRunFacet(
                        run=parent_run.Run(runId=self.openlineage_parent_run_id),
                        job=parent_run.Job(
                            namespace=self.openlineage_parent_job_namespace,  # type: ignore [arg-type]
                            name=self.openlineage_parent_job_name,  # type: ignore [arg-type]
                        ),
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
                "great_expectations_meta": GreatExpectationsRunFacet(  # type: ignore[dict-item]
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
            run=Run(runId=str(self.openlineage_run_id), facets=run_facets),
            job=Job(self.openlineage_namespace, job_name, facets=job_facets),  # type: ignore [arg-type]
            inputs=datasets,  # type: ignore[arg-type]
            outputs=[],
            producer="https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/common/openlineage/provider/great_expectations",
        )
        if self.do_publish:
            self.openlineage_client.emit(run_event)  # type: ignore [attr-defined]
        # Great expectations tries to append stuff here, so we need to make it a dict
        return Serde.to_dict(run_event)

    def _ser(self, obj):
        if hasattr(obj, "to_json_dict"):
            return obj.to_json_dict()
        else:
            return obj

    def _fetch_datasets_from_pandas_source(
        self,
        data_asset: Validator,  # Modern GE v3 Validator only
        validation_result_suite: ExpectationSuiteValidationResult,
    ) -> List[OLDataset]:
        """
        Generate a list of OpenLineage Datasets from a Validator with PandasExecutionEngine
        :param data_asset: Validator with PandasExecutionEngine
        :param validation_result_suite: Validation results
        :return: List of OpenLineage datasets
        """
        batch = data_asset.active_batch
        batch_data = batch.data  # type: ignore [union-attr]

        # Get the actual DataFrame from batch_data
        if hasattr(batch_data, "dataframe"):
            df = batch_data.dataframe  # type: ignore [union-attr]
        elif hasattr(batch_data, "_dataframe"):
            df = batch_data._dataframe  # type: ignore [union-attr]
        else:
            # Fallback: try to access the data directly
            df = batch_data

        # Get path from batch request options
        path = None
        if hasattr(batch, "batch_request") and hasattr(batch.batch_request, "options"):  # type: ignore [union-attr]
            path = batch.batch_request.options.get("path")  # type: ignore [union-attr]

        columns = [
            Field(
                name=col,
                type=str(df[col].dtype) if df[col].dtype is not None else "UNKNOWN",
            )
            for col in df.columns
        ]

        if path is None:
            # Handle in-memory DataFrame (no file path)
            log.info("Using in-memory DataFrame for Pandas dataset")
            # For in-memory DataFrames, use a generic identifier
            return [
                Dataset(
                    source=Source(scheme="memory", authority="pandas", connection_url="memory://pandas"),
                    name="dataframe",
                    fields=columns,
                    input_facets=self.results_facet(validation_result_suite),
                ).to_openlineage_dataset()  # type: ignore[list-item]
            ]

        if path.startswith("/"):
            path = f"file://{path}"

        parsed_url = urlparse(path)

        return [
            Dataset(
                source=self._source(parsed_url._replace(path="")),
                name=parsed_url.path,
                fields=columns,
                input_facets=self.results_facet(validation_result_suite),
            ).to_openlineage_dataset()  # type: ignore[list-item]
        ]

    def _fetch_datasets_from_sql_source(
        self,
        data_asset: Validator,  # Modern GE v3 Validator only
        validation_result_suite: ExpectationSuiteValidationResult,
    ) -> List[OLDataset]:
        """
        Generate a list of OpenLineage Datasets from a Validator with SqlAlchemyExecutionEngine.
        :param data_asset: Validator with SqlAlchemyExecutionEngine
        :param validation_result_suite: Validation results
        :return: List of OpenLineage datasets
        """
        metadata = MetaData()
        batch = data_asset.active_batch
        batch_data = batch.data  # type: ignore [union-attr]

        # Check for custom SQL query in various locations based on GE v3 API
        custom_sql = None

        # For GE v3 query assets, the query is in the data asset directly
        if hasattr(batch, "data_asset") and hasattr(batch.data_asset, "query"):  # type: ignore [union-attr]
            custom_sql = batch.data_asset.query  # type: ignore [union-attr]
        elif hasattr(batch, "_data_asset") and hasattr(batch._data_asset, "query"):  # type: ignore [union-attr]
            custom_sql = batch._data_asset.query  # type: ignore [union-attr]
        # Legacy path - check batch request and batch definition
        elif hasattr(batch, "batch_request"):
            if hasattr(batch.batch_request, "options") and batch.batch_request.options:  # type: ignore [union-attr]
                custom_sql = batch.batch_request.options.get("query")  # type: ignore [union-attr]
            elif (
                hasattr(batch.batch_request, "runtime_parameters") and batch.batch_request.runtime_parameters  # type: ignore [union-attr]
            ):
                custom_sql = batch.batch_request.runtime_parameters.get("query")  # type: ignore [union-attr]

        # Also check batch_definition for query
        if not custom_sql and hasattr(batch, "batch_definition"):
            batch_def = batch.batch_definition  # type: ignore [union-attr]
            if hasattr(batch_def, "data_asset") and hasattr(batch_def.data_asset, "query"):
                custom_sql = batch_def.data_asset.query

        if custom_sql:
            # Parse custom SQL to extract table lineage
            parsed_sql = parse(custom_sql, dialect=data_asset.execution_engine.dialect_name)  # type: ignore [attr-defined]
            if parsed_sql:
                return [
                    self._get_sql_table(batch_data, metadata, t.schema, t.name, validation_result_suite)  # type: ignore [arg-type,misc]
                    for t in parsed_sql.in_tables
                ]
            return []

        # Get table information from batch spec
        batch_spec = getattr(batch, "batch_spec", {})
        if isinstance(batch_spec, dict):
            table_name = batch_spec.get("table_name")
            schema_name = batch_spec.get("schema_name")
        else:
            # Handle batch_spec as object
            table_name = getattr(batch_spec, "table_name", None)
            schema_name = getattr(batch_spec, "schema_name", None)

        if not table_name:
            log.warning("No table_name found in batch_spec for SQL dataset")
            return []

        dataset = self._get_sql_table(
            batch_data,  # type: ignore [arg-type]
            metadata,
            schema_name,
            table_name,
            validation_result_suite,
        )
        return [dataset] if dataset else []

    def _get_sql_table(
        self,
        batch_data: SqlAlchemyBatchData,  # Modern GE v3 SqlAlchemyBatchData only
        meta: MetaData,
        schema: Optional[str],
        table_name: str,
        validation_result_suite: ExpectationSuiteValidationResult,
    ) -> Optional[OLDataset]:
        """
        Construct a Dataset from the connection url and the columns returned from SqlAlchemyBatchData
        :param batch_data: SqlAlchemyBatchData from the active batch
        :param meta: SQLAlchemy MetaData object
        :param schema: Schema name (optional)
        :param table_name: Table name
        :param validation_result_suite: Validation results
        :return: OpenLineage dataset
        """
        engine = batch_data._engine
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
        ).to_openlineage_dataset()  # type: ignore[return-value]

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
            log.exception("Exception while retrieving great expectations dataset")
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
                for parser in COLUMN_EXPECTATIONS_PARSER:  # type: ignore[assignment]
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data["columnMetrics"][result.column_id][result.facet_key] = result.value

            for key in facet_data["columnMetrics"].keys():
                facet_data["columnMetrics"][key] = data_quality_metrics_input_dataset.ColumnMetrics(
                    **facet_data["columnMetrics"][key]
                )
            return data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet(**facet_data)  # type: ignore[arg-type]
        except ValueError:
            log.exception("Great Expectations's CheckpointResult object does not have expected key")
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
            log.exception("Great Expectations's CheckpointResult object does not have expected key")
        return None
