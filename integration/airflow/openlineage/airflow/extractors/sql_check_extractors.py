# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from typing import Any, Dict, List, Optional

from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.client.facet_v2 import (
    BaseFacet,
    data_quality_assertions_dataset,
    data_quality_metrics_input_dataset,
)


def get_check_extractors(super_):
    class BaseSqlCheckExtractor(super_):
        def __init__(self, operator):
            super().__init__(operator)

        def extract(self) -> TaskMetadata:
            pass

        def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
            from openlineage.airflow.extractors.bigquery_extractor import (
                BigQueryExtractor,
            )

            if issubclass(BigQueryExtractor, BaseSqlCheckExtractor):
                return super().extract_on_complete(task_instance)
            return super().extract()

    class SqlCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ["SQLCheckOperator"]

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            return {}

    class SqlValueCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ["SQLValueCheckOperator"]

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            return {}

    class SqlThresholdCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ["SQLThresholdCheckOperator"]

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            return {}

    class SqlIntervalCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ["SQLIntervalCheckOperator"]

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            return {}

    class SqlColumnCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ["SQLColumnCheckOperator", "BigQueryColumnCheckOperator"]

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            """
            Function should expect the column_mapping to take the following form:
            {
                'col_name': {
                    'null_check': {
                        'pass_value': 0,
                        'result': 0,
                        'success': True
                    },
                    'min': {
                        'pass_value': 5,
                        'tolerance': 0.2,
                        'result': 1,
                        'success': False
                    }
                }
            }
            """

            def map_facet_name(check_name) -> str:
                if "null" in check_name:
                    return "nullCount"
                elif "distinct" in check_name:
                    return "distinctCount"
                elif "sum" in check_name:
                    return "sum"
                elif "count" in check_name:
                    return "count"
                elif "min" in check_name:
                    return "min"
                elif "max" in check_name:
                    return "max"
                elif "quantiles" in check_name:
                    return "quantiles"
                return ""

            facet_data: Dict[str, Any] = {"columnMetrics": defaultdict(dict)}
            assertion_data: Dict[str, List[data_quality_assertions_dataset.Assertion]] = {"assertions": []}
            for col_name, checks in self.operator.column_mapping.items():
                col_name = col_name.upper() if self._is_uppercase_names else col_name
                for check, check_values in checks.items():
                    facet_key = map_facet_name(check)
                    facet_data["columnMetrics"][col_name][facet_key] = check_values.get("result")

                    assertion_data["assertions"].append(
                        data_quality_assertions_dataset.Assertion(
                            assertion=check,
                            success=check_values.get("success"),
                            column=col_name,
                        )
                    )
                facet_data["columnMetrics"][col_name] = data_quality_metrics_input_dataset.ColumnMetrics(
                    **facet_data["columnMetrics"][col_name]
                )

            data_quality_facet = data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet(
                **facet_data
            )
            data_quality_assertions_facet = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                assertions=assertion_data["assertions"]
            )

            return {
                "dataQuality": data_quality_facet,
                "dataQualityMetrics": data_quality_facet,
                "dataQualityAssertions": data_quality_assertions_facet,
            }

    class SqlTableCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return ["SQLTableCheckOperator", "BigQueryTableCheckOperator"]

        def _get_input_facets(self) -> Dict[str, BaseFacet]:
            """
            Function should expect to take the checks in the following form:
            {
                'row_count_check': {
                    'pass_value': 100,
                    'tolerance': .05,
                    'result': 101,
                    'success': True
                }
            }
            """
            facet_data: Dict[str, Any] = {"columnMetrics": defaultdict(dict)}
            assertion_data: Dict[str, List[data_quality_assertions_dataset.Assertion]] = {"assertions": []}
            for check, check_values in self.operator.checks.items():
                assertion_data["assertions"].append(
                    data_quality_assertions_dataset.Assertion(
                        assertion=check,
                        success=check_values.get("success"),
                    )
                )
            facet_data["rowCount"] = self.operator.checks.get("row_count_check", {}).get("result", None)
            facet_data["bytes"] = self.operator.checks.get("bytes", {}).get("result", None)

            data_quality_facet = data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet(
                **facet_data
            )
            data_quality_assertions_facet = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                assertions=assertion_data["assertions"]
            )

            return {
                "dataQuality": data_quality_facet,
                "dataQualityMetrics": data_quality_facet,
                "dataQualityAssertions": data_quality_assertions_facet,
            }

    return [
        SqlCheckExtractor,
        SqlValueCheckExtractor,
        SqlThresholdCheckExtractor,
        SqlIntervalCheckExtractor,
        SqlColumnCheckExtractor,
        SqlTableCheckExtractor,
    ]
