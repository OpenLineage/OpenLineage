# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Dict

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.client.facet import BaseFacet
from openlineage.airflow.utils import (
    build_column_check_facets,
    build_table_check_facets
)

class BaseSqlCheckExtractor(SqlExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> TaskMetadata:
        return

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        return super().extract()


class SqlCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SqlValueCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLValueCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SqlThresholdCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLThresholdCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SqlIntervalCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLIntervalCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}


class SqlColumnCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLColumnCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        column_mapping = self.operator.column_mapping
        return build_column_check_facets(column_mapping)


class SqlTableCheckExtractor(BaseSqlCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SQLTableCheckOperator']

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        checks = self.operator.checks
        return build_table_check_facets(checks)
