# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Dict

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.utils import get_connection_uri  # noqa
from openlineage.client.facet import BaseFacet, ExternalQueryRunFacet
from openlineage.common.sql import DbTableMeta


logger = logging.getLogger(__file__)


class SnowflakeExtractor(SqlExtractor):
    source_type = "SNOWFLAKE"
    default_schema = "PUBLIC"

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeOperator", "SnowflakeOperatorAsync"]

    def _get_in_query(self, in_tables):
        return self._information_schema_query(in_tables)

    def _get_out_query(self, out_tables):
        return self._information_schema_query(out_tables)

    def _information_schema_query(self, tables: List[DbTableMeta]) -> str:
        table_names = ",".join(
            map(lambda name: f"'{self._normalize_identifiers(name.name)}'", tables)
        )
        database = self.operator.database
        if not database:
            database = self._get_database()
        sql = f"""
        SELECT table_schema,
               table_name,
               column_name,
               ordinal_position,
               data_type
          FROM {database}.information_schema.columns
         WHERE table_name IN ({table_names});
        """
        return sql

    def _get_database(self) -> str:
        if hasattr(self.operator, "database") and self.operator.database is not None:
            return self.operator.database
        return self.conn.extra_dejson.get(
            "extra__snowflake__database", ""
        ) or self.conn.extra_dejson.get("database", "")

    def _get_authority(self) -> str:
        if hasattr(self.operator, "account") and self.operator.account is not None:
            return self.operator.account
        return self.conn.extra_dejson.get(
            "extra__snowflake__account", ""
        ) or self.conn.extra_dejson.get("account", "")

    def _get_hook(self):
        if hasattr(self.operator, "get_db_hook"):
            return self.operator.get_db_hook()
        else:
            return self.operator.get_hook()

    def _normalize_identifiers(self, table: str):
        """
        Snowflake keeps it's table names in uppercase, so we need to normalize
        them before use: see
        https://community.snowflake.com/s/question/0D50Z00009SDHEoSAP/is-there-case-insensitivity-for-table-name-or-column-names  # noqa
        """
        return table.upper()

    def _get_query_ids(self) -> List[str]:
        if hasattr(self.operator, "query_ids"):
            return self.operator.query_ids
        return []

    def _get_scheme(self):
        return "snowflake"

    def _get_connection_uri(self):
        return get_connection_uri(self.conn)

    def _get_db_specific_run_facets(self, source, *_) -> Dict[str, BaseFacet]:
        query_ids = self._get_query_ids()
        run_facets = {}
        if len(query_ids) == 1:
            run_facets["externalQuery"] = ExternalQueryRunFacet(
                externalQueryId=query_ids[0], source=source.name
            )
        elif len(query_ids) > 1:
            logger.warning(
                "Found more than one query id for task "
                f"{self.operator.dag_id}.{self.operator.task_id}: {query_ids} "
                "This might indicate that this task might be better as multiple jobs"
            )
        return run_facets
