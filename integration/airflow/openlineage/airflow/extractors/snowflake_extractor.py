# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List

from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor
from openlineage.airflow.utils import get_connection_uri, get_connection  # noqa

log = logging.getLogger(__file__)


class SnowflakeExtractor(PostgresExtractor):
    source_type = 'SNOWFLAKE'

    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['SnowflakeOperator']

    def _information_schema_query(self, table_names: str) -> str:
        database = self.operator.database
        if not database:
            database = self._get_database()
        return f"""
        SELECT table_schema,
               table_name,
               column_name,
               ordinal_position,
               data_type
          FROM {database}.information_schema.columns
         WHERE table_name IN ({table_names});
        """

    def _get_scheme(self):
        return 'snowflake'

    def _get_database(self) -> str:
        return self._get_hook()._get_conn_params()['database']

    def _get_authority(self) -> str:
        return self._get_hook()._get_conn_params()['account']

    def _get_hook(self):
        if hasattr(self.operator, 'get_db_hook'):
            return self.operator.get_db_hook()
        else:
            return self.operator.get_hook()

    def _conn_id(self):
        return self.operator.snowflake_conn_id

    def _normalize_identifiers(self, table: str):
        """
        Snowflake keeps it's table names in uppercase, so we need to normalize
        them before use: see
        https://community.snowflake.com/s/question/0D50Z00009SDHEoSAP/is-there-case-insensitivity-for-table-name-or-column-names  # noqa
        """
        return table.upper()

    def _get_connection_uri(self):
        return get_connection_uri(self.conn)

    def _get_query_ids(self) -> List[str]:
        if hasattr(self.operator, 'query_ids'):
            return self.operator.query_ids
        return []
