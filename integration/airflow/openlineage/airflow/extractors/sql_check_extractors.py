# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Dict

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
from openlineage.client.facet import BaseFacet
from openlineage.airflow.utils import (
    build_column_check_facets,
    build_table_check_facets
)


def get_check_extractors(super_):
    class BaseSqlCheckExtractor(super_):
        def __init__(self, operator):
            super().__init__(operator)

        def extract(self) -> TaskMetadata:
            pass

        def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
            if issubclass(BigQueryExtractor, BaseSqlCheckExtractor):
                return super().extract_on_complete(task_instance)
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
            return ['SQLColumnCheckOperator', 'BigQueryColumnCheckOperator']

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            column_mapping = self.operator.column_mapping
            return build_column_check_facets(column_mapping)

    class SqlTableCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ['SQLTableCheckOperator', 'BigQueryTableCheckOperator']

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            checks = self.operator.checks
            return build_table_check_facets(checks)

    return [
        SqlCheckExtractor,
        SqlValueCheckExtractor,
        SqlThresholdCheckExtractor,
        SqlIntervalCheckExtractor,
        SqlColumnCheckExtractor,
        SqlTableCheckExtractor
    ]
