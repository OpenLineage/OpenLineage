import logging
import traceback

from abc import abstractmethod
from sqlalchemy import MetaData, Table
from typing import List, Optional

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
from openlineage.airflow.utils import get_job_name
from openlineage.client.facet import SqlJobFacet
from openlineage.common.dataset import Dataset, Field
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider, BigQueryErrorRunFacet
from facet_builders import (
    build_check_facets,
    build_value_check_facets,
    build_interval_check_facets,
    build_threshold_check_facets
)

logger = logging.getLogger(__name__)


class BaseBigQueryCheckExtractor(BigQueryExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        logger.debug(f"extract_on_complete({task_instance})")
        context = self.parse_sql_context()

        try:
            bigquery_job_id = self._get_xcom_bigquery_job_id(task_instance)
            if bigquery_job_id is None:
                raise Exception("Xcom could not resolve BigQuery job id."
                                + "Job may have failed.")
        except Exception as e:
            logger.error(f"Cannot retrieve job details from BigQuery.Client. {e}",
                         exc_info=True)
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                run_facets={
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {traceback.format_exc()}",
                        parserError=context.parser_error
                    )
                }
            )

        client = self._get_client()

        stats = BigQueryDatasetsProvider(client=client).get_facets(bigquery_job_id)
        inputs = stats.inputs
        for ds in inputs:
            ds.input_facets = self._build_facets()
        output = stats.output
        run_facets = stats.run_facets
        job_facets = {
            "sql": SqlJobFacet(context.sql)
        }

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[output.to_openlineage_dataset()] if output else [],
            run_facets=run_facets,
            job_facets=job_facets
        )

    @abstractmethod
    def _build_facets(self) -> dict:
        pass


class BigQueryCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryCheckOperator']

    def _build_facets(self) -> dict:
        return build_check_facets()


class BigQueryValueCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryValueCheckOperator']

    def _build_facets(self) -> dict:
        return build_value_check_facets()


class BigQueryThresholdCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryThresholdCheckOperator']

    def _build_facets(self) -> dict:
        return build_threshold_check_facets()


class BigQueryIntervalCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryIntervalCheckOperator']

    def _build_facets(self) -> dict:
        return build_interval_check_facets()
