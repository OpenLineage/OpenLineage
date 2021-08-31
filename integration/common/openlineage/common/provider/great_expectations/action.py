import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Optional, List
from urllib.parse import urlparse
from uuid import uuid4

from great_expectations.checkpoint import ValidationAction
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier
from great_expectations.dataset import SqlAlchemyDataset, PandasDataset, Dataset as GEDataset
from openlineage.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import ParentRunFacet, DocumentationJobFacet, \
    SourceCodeLocationJobFacet
from openlineage.client.run import RunEvent, RunState, Run, Job
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Connection

from openlineage.common.dataset import Dataset, Source, Field
from openlineage.common.dataset import Dataset as OLDataset
from openlineage.common.models import ColumnMetric, DataQualityDatasetFacet
from openlineage.common.provider.great_expectations.facets import \
    GreatExpectationsAssertionsDatasetFacet, \
    GreatExpectationsRunFacet
from openlineage.common.provider.great_expectations.results import EXPECTATIONS_PARSERS, \
    COLUMN_EXPECTATIONS_PARSER, \
    GreatExpectationsAssertion
from openlineage.common.sql import SqlParser


class OpenLineageValidationAction(ValidationAction):
    """
    ValidationAction implementation which posts RunEvents for a GreatExpectations validation job.

    Openlineage host parameters can be passed in as constructor arguments or environment variables
    will be searched. Job information can optionally be passed in as constructor arguments or the
    great expectations suite name will be used as the job name (the namespace should be passed in
    as either a constructor arg or as an environment variable).

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

    def __init__(self, data_context,
                 openlineage_host=None,
                 openlineage_namespace=None,
                 openlineage_apiKey=None,
                 openlineage_parent_run_id=None,
                 openlineage_parent_job_namespace=None,
                 openlineage_parent_job_name=None,
                 job_name=None,
                 job_description=None,
                 code_location=None,
                 openlineage_run_id=None):
        super().__init__(data_context)
        if openlineage_host is not None:
            self.openlineage_client = OpenLineageClient(openlineage_host,
                                                        OpenLineageClientOptions(
                                                            api_key=openlineage_apiKey))
        else:
            self.openlineage_client = OpenLineageClient.from_environment()
        if openlineage_namespace is not None:
            self.namespace = openlineage_namespace
        else:
            self.namespace = os.getenv('OPENLINEAGE_NAMESPACE', 'default')
        if openlineage_run_id is not None:
            self.run_id = openlineage_run_id
        else:
            self.run_id = uuid4()
        self.parent_run_id = openlineage_parent_run_id
        self.parent_job_namespace = openlineage_parent_job_namespace
        self.parent_job_name = openlineage_parent_job_name
        self.job_name = job_name
        self.job_description = job_description
        self.code_location = code_location
        self.log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult,
             validation_result_suite_identifier: ValidationResultIdentifier,
             data_asset: GEDataset,
             payload=None):
        datasets = []
        if isinstance(data_asset, SqlAlchemyDataset):
            datasets = self._fetch_datasets_from_sql_source(data_asset, validation_result_suite)
        elif isinstance(data_asset, PandasDataset):
            datasets = self._fetch_datasets_from_pandas_source(data_asset, validation_result_suite)
        run_facets = {}
        if self.parent_run_id is not None:
            run_facets.update({"parentRun": ParentRunFacet.create(
                self.parent_run_id,
                self.parent_job_namespace,
                self.parent_job_name
            )})
        run_facets.update(
            {"great_expectations_meta": GreatExpectationsRunFacet(**validation_result_suite.meta)})
        job_facets = {}
        if self.job_description:
            job_facets.update({
                "documentation": DocumentationJobFacet(self.job_description)
            })
        if self.code_location:
            job_facets.update({
                "sourceCodeLocation": SourceCodeLocationJobFacet("", self.code_location)
            })

        job_name = self.job_name
        if self.job_name is None:
            job_name = validation_result_suite.meta["expectation_suite_name"]
        run_event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat(),
            run=Run(runId=str(self.run_id), facets=run_facets),
            job=Job(self.namespace, job_name, facets=job_facets),
            inputs=datasets,
            outputs=[],
            producer="https://github.com/OpenLineage/OpenLineage/tree/$VERSION/integration/common/openlineage/provider/great_expectations" # noqa
        )
        self.openlineage_client.emit(run_event)
        return run_event

    def _fetch_datasets_from_pandas_source(self, data_asset: PandasDataset,
                                           validation_result_suite: ExpectationSuiteValidationResult) -> List[OLDataset]: # noqa
        """
        Generate a list of OpenLineage Datasets from a PandasDataset
        :param data_asset:
        :param validation_result_suite:
        :return:
        """
        if data_asset.batch_kwargs.__contains__("path"):
            path = data_asset.batch_kwargs.get("path")
            if path.startswith("/"):
                path = "file://{}".format(path)
            parsed_url = urlparse(path)
            columns = [Field(
                name=col,
                type=str(data_asset[col].dtype) if data_asset[col].dtype is not None else 'UNKNOWN'
            ) for col in data_asset.columns]
            return [
                Dataset(
                    source=self._source(parsed_url._replace(path='')),
                    name=parsed_url.path,
                    fields=columns,
                    input_facets=self.results_facet(validation_result_suite)
                ).to_openlineage_dataset()
            ]

    def _fetch_datasets_from_sql_source(self, data_asset: SqlAlchemyDataset,
                                        validation_result_suite: ExpectationSuiteValidationResult) -> List[OLDataset]: # noqa
        """
        Generate a list of OpenLineage Datasets from a SqlAlchemyDataset.
        :param data_asset:
        :param validation_result_suite:
        :return:
        """
        metadata = MetaData()
        if data_asset.generated_table_name is not None:
            custom_sql = data_asset.batch_kwargs.get('query')
            parsed_sql = SqlParser.parse(custom_sql)
            return [
                self._get_sql_table(data_asset, metadata, t.schema, t.name,
                                    validation_result_suite) for t in
                parsed_sql.in_tables
            ]
        return [self._get_sql_table(data_asset, metadata, data_asset._table.schema,
                                    data_asset._table.name,
                                    validation_result_suite)]

    def _get_sql_table(self, data_asset: SqlAlchemyDataset,
                       meta: MetaData,
                       schema: str,
                       table_name: str,
                       validation_result_suite: ExpectationSuiteValidationResult) -> Optional[OLDataset]: # noqa
        """
        Construct a Dataset from the connection url and the columns returned from the
        SqlAlchemyDataset
        :param data_asset:
        :return:
        """
        engine = data_asset.engine
        if isinstance(engine, Connection):
            engine = engine.engine
        datasource_url = engine.url
        if engine.dialect.name.lower() == "bigquery":
            schema = '{}.{}'.format(datasource_url.host, datasource_url.database)

        table = Table(table_name, meta, autoload_with=engine)

        fields = [Field(
            name=key,
            type=str(col.type) if col.type is not None else 'UNKNOWN',
            description=col.doc
        ) for key, col in table.columns.items()]

        name = table_name \
            if schema is None \
            else "{}.{}".format(schema, table_name)

        results_facet = self.results_facet(validation_result_suite)
        return Dataset(
            source=self._source(urlparse(str(datasource_url))),
            fields=fields,
            name=name,
            input_facets=results_facet
        ).to_openlineage_dataset()

    def _source(self, url) -> Source:
        """
        Construct a Source from the connection url. Special handling for BigQuery is included.
        We attempt to strip credentials from the connection url, if present.
        :param url: a parsed url, as returned from urlparse()
        :return:
        """

        if url.scheme == "bigquery":
            return Source(
                scheme='bigquery',
                connection_url='bigquery'
            )

        return Source(
            scheme=url.scheme,
            authority=url.hostname,
            # Remove credentials from the URL if present
            connection_url=url._replace(netloc=url.hostname, query=None, fragment=None).geturl()
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
                'dataQuality': data_quality_facet,
                'greatExpectations_assertions': assertions_facet,
                'dataQualityMetrics': data_quality_facet.to_openlineage()
            }

        except ValueError:
            self.log.exception("Exception while retrieving great expectations dataset")
        return None

    def parse_data_quality_facet(self, validation_result: ExpectationSuiteValidationResult) \
            -> Optional[DataQualityDatasetFacet]:
        """
        Parse the validation result and extract a DataQualityDatasetFacet
        :param validation_result:
        :return:
        """
        facet_data = {
            "columnMetrics": defaultdict(dict)
        }

        # try to get to actual expectations results
        try:
            expectations_results = validation_result['results']
            for expectation in expectations_results:
                for parser in EXPECTATIONS_PARSERS:

                    # accept possible duplication, should have no difference in results
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data[result.facet_key] = result.value
                for parser in COLUMN_EXPECTATIONS_PARSER:
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data['columnMetrics'][result.column_id][result.facet_key] \
                            = result.value

            for key in facet_data['columnMetrics'].keys():
                facet_data['columnMetrics'][key] = ColumnMetric(**facet_data['columnMetrics'][key])
            return DataQualityDatasetFacet(**facet_data)
        except ValueError:
            self.log.exception(
                "Great Expectations's CheckpointResult object does not have expected key"
            )
        return None

    def parse_assertions(self, validation_result: ExpectationSuiteValidationResult) -> \
            Optional[GreatExpectationsAssertionsDatasetFacet]:
        assertions = []

        try:
            for expectation in validation_result.results:
                assertions.append(GreatExpectationsAssertion(
                    expectationType=expectation['expectation_config']['expectation_type'],
                    success=expectation['success'],
                    column=expectation['expectation_config']['kwargs'].get('column', None)
                ))

            return GreatExpectationsAssertionsDatasetFacet(assertions)
        except ValueError:
            self.log.exception(
                "Great Expectations's CheckpointResult object does not have expected key"
            )
        return None
