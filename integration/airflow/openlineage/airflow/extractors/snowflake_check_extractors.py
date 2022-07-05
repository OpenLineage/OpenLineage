# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Dict

from openlineage.airflow.extractors.sql_check_extractors import BaseSqlCheckExtractor
from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.client.facet import BaseFacet
from openlineage.airflow.utils import (
    build_column_check_facets,
    build_table_check_facets
)


class BaseSnowflakeCheckExtractor(BaseSqlCheckExtractor, SnowflakeExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> TaskMetadata:
        return

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        return super().extract()


class SnowflakeCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeCheckOperator"]

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SnowflakeValueCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeValueCheckOperator"]

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SnowflakeThresholdCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeThresholdCheckOperator"]

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SnowflakeIntervalCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeIntervalCheckOperator"]

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SnowflakeColumnCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeColumnCheckOperator"]

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        column_mapping = self.operator.column_mapping
        return build_column_check_facets(column_mapping)


class SnowflakeTableCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeTableCheckOperator"]

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        checks = self.operator.checks
        return build_table_check_facets(checks)
