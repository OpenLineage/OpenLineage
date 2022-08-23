# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Dict

from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.airflow.utils import get_connection_uri  # noqa
from openlineage.client.facet import BaseFacet, ExternalQueryRunFacet


logger = logging.getLogger(__file__)


class SnowflakeExtractor(SqlExtractor):
    source_type = "SNOWFLAKE"
    default_schema = "PUBLIC"
    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
    ]
    _is_information_schema_cross_db = True
    _is_case_sensitive = True

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeOperator", "SnowflakeOperatorAsync"]

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
