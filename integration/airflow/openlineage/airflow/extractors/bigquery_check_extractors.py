import logging

from abc import abstractmethod
from typing import List

from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
from utils import (
    build_column_check_facets,
    build_table_check_facets,
)

logger = logging.getLogger(__name__)


class BaseBigQueryCheckExtractor(BigQueryExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)

    def _get_inputs(self, stats):
        inputs = stats.inputs
        for ds in inputs:
            ds.input_facets = self._build_facets()
        return inputs

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
        pass


class BigQueryValueCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryValueCheckOperator']

    def _build_facets(self) -> dict:
        pass


class BigQueryThresholdCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryThresholdCheckOperator']

    def _build_facets(self) -> dict:
        pass


class BigQueryIntervalCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryIntervalCheckOperator']

    def _build_facets(self) -> dict:
        pass


class BigQueryColumnCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryColumnCheckOperator']

    def _build_facets(self) -> dict:
        column_mapping = self.operator.column_mapping
        return build_column_check_facets(column_mapping)


class BigQueryTableCheckExtractor(BaseBigQueryCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['BigQueryTableCheckOperator']

    def _build_facets(self) -> dict:
        checks = self.operator.checks
        return build_table_check_facets(checks)
