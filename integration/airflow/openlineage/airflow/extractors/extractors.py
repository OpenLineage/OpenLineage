# SPDX-License-Identifier: Apache-2.0

import os

from typing import Type, Optional

from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.airflow.utils import import_from_string, try_import_from_string

_extractors = list(
    filter(
        lambda t: t is not None,
        [
            try_import_from_string(
                'openlineage.airflow.extractors.postgres_extractor.PostgresExtractor'
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.mysql_extractor.MySqlExtractor'
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.bigquery_extractor.BigQueryExtractor'
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.great_expectations_extractor.GreatExpectationsExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.snowflake_extractor.SnowflakeExtractor'
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.python_extractor.PythonExtractor'
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.bash_extractor.BashExtractor'
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.sql_check_extractors.SqlCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.sql_check_extractors.SqlValueCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.sql_check_extractors.SqlIntervalCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.sql_check_extractors.SqlThresholdCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.snowflake_check_extractors.SnowflakeCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.snowflake_check_extractors.SnowflakeValueCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.snowflake_check_extractors.SnowflakeIntervalCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.snowflake_check_extractors.SnowflakeThresholdCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.bigquery_check_extractors.BigQueryCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.bigquery_check_extractors.BigQueryIntervalCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.bigquery_check_extractors.BigQueryValueCheckExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'openlineage.airflow.extractors.bigquery_check_extractors.BigQueryThresholdCheckExtractor'  # noqa: E501
            ),
        ],
    )
)


class Extractors:
    """
    This exposes implemented extractors, while hiding ones that require additional, unmet
    dependency. Patchers are a category of extractor that needs to hook up to operator's
    internals during DAG creation.
    """

    def __init__(self):
        # Do not expose extractors relying on external dependencies that are not installed
        self.extractors = {}

        for extractor in _extractors:
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

        # Comma-separated extractors in OPENLINEAGE_EXTRACTORS variable.
        # Extractors should implement BaseExtractor
        env_extractors = os.getenv("OPENLINEAGE_EXTRACTORS")
        if env_extractors is not None:
            for extractor in env_extractors.split(';'):
                extractor = import_from_string(extractor)
                for operator_class in extractor.get_operator_classnames():
                    self.extractors[operator_class] = extractor

        # Previous way of adding extractors
        # Adding operator: extractor pairs registered via environmental variable in pattern
        # OPENLINEAGE_EXTRACTOR_<operator>=<path.to.ExtractorClass>
        # The value in extractor map is extractor class type - it needs to be instantiated.
        # We import the module provided and get type using importlib then.
        for key, value in os.environ.items():
            if key.startswith("OPENLINEAGE_EXTRACTOR_"):
                extractor = import_from_string(value)
                for operator_class in extractor.get_operator_classnames():
                    self.extractors[operator_class] = extractor

    def add_extractor(self, operator: str, extractor: Type):
        self.extractors[operator] = extractor

    def get_extractor_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        if name in self.extractors:
            return self.extractors[name]
        return None
