import logging

from abc import abstractmethod
from typing import List

from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
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
