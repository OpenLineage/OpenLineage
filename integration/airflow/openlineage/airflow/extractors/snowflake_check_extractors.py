import logging
from abc import abstractmethod
from typing import List

from openlineage.airflow.extractors.snowflake_extractor import SnowflakeExtractor
from openlineage.common.dataset import Dataset, Field
from openlineage.airflow.utils import (
    build_check_facets,
    build_value_check_facets,
    build_interval_check_facets,
    build_threshold_check_facets
)
from sqlalchemy import MetaData, Table

logger = logging.getLogger(__name__)


class BaseSnowflakeCheckExtractor(SnowflakeExtractor):
    default_schema = 'public'

    def __init__(self, operator):
        super().__init__(operator)

    def _get_input_tables(self, source, database, sql_meta):
        inputs = []
        for in_table_schema in self._get_table_schemas(sql_meta.in_tables):
            table_name = self._normalize_identifiers(in_table_schema.table_name.name)
            ds = Dataset.from_table(
                source=source,
                table_name=table_name,
                schema_name=in_table_schema.schema_name,
                database_name=database
            )
            ds.input_facets = self._build_facets()

            table = Table(
                table_name,
                MetaData(),
                autoload_with=self._get_engine()
            )
            ds.fields = [
                Field(
                    name=key,
                    type=str(col.type) if col.type is not None else 'UNKNOWN',
                    description=col.doc
                ) for key, col in table.columns.items()
            ]

            inputs.append(ds)
        return [ds.to_openlineage_dataset() for ds in inputs]

    def _get_output_tables(self, source, database, sql_meta):
        return []

    @abstractmethod
    def _build_facets(self) -> dict:
        pass


class SnowflakeCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeCheckOperator']

    def _build_facets(self) -> dict:
        return build_check_facets()


class SnowflakeValueCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeValueCheckOperator']

    def _build_facets(self) -> dict:
        return build_value_check_facets()


class SnowflakeThresholdCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeThresholdCheckOperator']

    def _build_facets(self) -> dict:
        return build_threshold_check_facets()


class SnowflakeIntervalCheckExtractor(BaseSnowflakeCheckExtractor):
    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeIntervalCheckOperator']

    def _build_facets(self) -> dict:
        return build_interval_check_facets()
