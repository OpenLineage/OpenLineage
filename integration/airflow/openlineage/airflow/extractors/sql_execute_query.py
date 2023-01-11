# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

sql_extractors = {
    "bigquery": "BigQueryExtractor",
    "mysql": "MysqlExtractor",
    "postgres": "PostgresExtractor",
    "redshift": "RedshiftExtractor",
    "snowflake": "SnowflakeExtractor",
}


def get_sql_execute_query_extractor(super_):
    class SQLExecuteQueryExtractor(super_):
        pass

    return SQLExecuteQueryExtractor
