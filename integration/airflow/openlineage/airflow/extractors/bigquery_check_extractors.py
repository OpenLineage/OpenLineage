# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Dict

from openlineage.airflow.extractors.sql_check_extractors import SqlCheckExtractor
from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.client.facet import BaseFacet
from openlineage.airflow.utils import (
    build_column_check_facets,
    build_table_check_facets
)


class BaseBigQueryCheckExtractor(SqlCheckExtractor, BigQueryExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> TaskMetadata:
        return

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        return super().extract()


class BigQueryCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class BigQueryValueCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryValueCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class BigQueryThresholdCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryThresholdCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class BigQueryIntervalCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryIntervalCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class BigQueryColumnCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryColumnCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        column_mapping = self.operator.column_mapping
        return build_column_check_facets(column_mapping)


class BigQueryTableCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryTableCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        checks = self.operator.checks
        return build_table_check_facets(checks)
