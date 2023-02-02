# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, List
from urllib.parse import urlparse

from openlineage.airflow.extractors.dbapi_utils import execute_query_on_hook
from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.client.facet import BaseFacet, ExternalQueryRunFacet
from openlineage.common.provider.snowflake import fix_snowflake_sqlalchemy_uri


class SnowflakeExtractor(SqlExtractor):
    source_type = "SNOWFLAKE"

    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
    ]
    _is_information_schema_cross_db = True
    _is_uppercase_names = True

    # extra prefix should be deprecated soon in Airflow
    _whitelist_query_params: List[str] = ["warehouse", "account", "database", "region"] + [
        "extra__snowflake__" + el
        for el in ["warehouse", "account", "database", "region"]
    ]

    @property
    def dialect(self):
        return "snowflake"

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["SnowflakeOperator", "SnowflakeOperatorAsync"]

    @property
    def default_schema(self):
        return execute_query_on_hook(self.hook, "SELECT current_schema();")[0][0]  # row -> column

    def _get_database(self) -> str:
        if hasattr(self.operator, "database") and self.operator.database is not None:
            return self.operator.database
        return self.conn.extra_dejson.get(
            "extra__snowflake__database", ""
        ) or self.conn.extra_dejson.get("database", "")

    def _get_authority(self) -> str:
        uri = fix_snowflake_sqlalchemy_uri(self.hook.get_uri())
        return urlparse(uri).hostname

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

    def _get_db_specific_run_facets(self, source, *_) -> Dict[str, BaseFacet]:
        query_ids = self._get_query_ids()
        run_facets = {}
        if len(query_ids) == 1:
            run_facets["externalQuery"] = ExternalQueryRunFacet(
                externalQueryId=query_ids[0], source=source.name
            )
        elif len(query_ids) > 1:
            self.log.warning(
                "Found more than one query id for task "
                f"{self.operator.dag_id}.{self.operator.task_id}: {query_ids} "
                "This might indicate that this task might be better as multiple jobs"
            )
        return run_facets
