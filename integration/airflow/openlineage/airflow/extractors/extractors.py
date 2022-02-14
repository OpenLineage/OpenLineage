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
        ],
    )
)

_patchers = list(
    filter(
        lambda t: t is not None,
        [
            try_import_from_string(
                'openlineage.airflow.extractors.great_expectations_extractor.GreatExpectationsExtractor'  # noqa: E501
            )
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
        self.patchers = {}

        for extractor in _extractors:
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

        for patcher in _patchers:
            for operator_class in patcher.get_operator_classnames():
                self.patchers[operator_class] = patcher

        # Adding operator: extractor pairs registered via environmental variable in pattern
        # OPENLINEAGE_EXTRACTOR_<operator>=<path.to.ExtractorClass>
        # The value in extractor map is extractor class type - it needs to be instantiated.
        # We import the module provided and get type using importlib then.
        for key, value in os.environ.items():
            if key.startswith("OPENLINEAGE_EXTRACTOR_"):
                operator = key[22:]
                self.extractors[operator] = import_from_string(value)

    def add_extractor(self, operator: str, extractor: Type):
        self.extractors[operator] = extractor

    def get_extractor_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        if name in self.extractors:
            return self.extractors[name]
        return None

    def get_patcher_class(self, clazz: Type) -> Optional[Type[BaseExtractor]]:
        name = clazz.__name__
        if name in self.patchers:
            return self.patchers[name]
        return None
