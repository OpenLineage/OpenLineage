# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Optional, Type

from openlineage.airflow.extractors.base import BaseExtractor, DefaultExtractor
from openlineage.airflow.extractors.sql_check_extractors import get_check_extractors
from openlineage.airflow.extractors.sql_execute_query import (
    get_sql_execute_query_extractor,
    sql_extractors,
)
from openlineage.airflow.utils import import_from_string, try_import_from_string

_extractors = list(
    filter(
        lambda t: t is not None,
        [
            try_import_from_string("openlineage.airflow.extractors.postgres_extractor.PostgresExtractor"),
            try_import_from_string("openlineage.airflow.extractors.mysql_extractor.MySqlExtractor"),
            try_import_from_string("openlineage.airflow.extractors.bigquery_extractor.BigQueryExtractor"),
            try_import_from_string(
                "openlineage.airflow.extractors.great_expectations_extractor.GreatExpectationsExtractor"
            ),
            try_import_from_string("openlineage.airflow.extractors.snowflake_extractor.SnowflakeExtractor"),
            try_import_from_string("openlineage.airflow.extractors.python_extractor.PythonExtractor"),
            try_import_from_string("openlineage.airflow.extractors.bash_extractor.BashExtractor"),
            try_import_from_string(
                "openlineage.airflow.extractors.redshift_sql_extractor.RedshiftSQLExtractor"
            ),
            try_import_from_string(
                "openlineage.airflow.extractors.redshift_data_extractor.RedshiftDataExtractor"
            ),
            try_import_from_string("openlineage.airflow.extractors.trino_extractor.TrinoExtractor"),
            try_import_from_string("openlineage.airflow.extractors.athena_extractor.AthenaExtractor"),
            try_import_from_string("openlineage.airflow.extractors.sftp_extractor.SFTPExtractor"),
            try_import_from_string("openlineage.airflow.extractors.ftp_extractor.FTPExtractor"),
            try_import_from_string(
                "openlineage.airflow.extractors.sagemaker_extractors.SageMakerProcessingExtractor"
            ),
            try_import_from_string(
                "openlineage.airflow.extractors.sagemaker_extractors.SageMakerTrainingExtractor"
            ),
            try_import_from_string(
                "openlineage.airflow.extractors.sagemaker_extractors.SageMakerTransformExtractor"
            ),
            try_import_from_string("openlineage.airflow.extractors.s3_extractor.S3CopyObjectExtractor"),
            try_import_from_string("openlineage.airflow.extractors.s3_extractor.S3FileTransformExtractor"),
            try_import_from_string("openlineage.airflow.extractors.gcs_extractor.GCSToGCSExtractor"),
            try_import_from_string("openlineage.airflow.extractors.dbt_cloud_extractor.DbtCloudExtractor"),
        ],
    )
)

_extractors += get_check_extractors(
    try_import_from_string("openlineage.airflow.extractors.sql_extractor.SqlExtractor")
)

_check_providers = {
    "PostgresExtractor": "postgres",
    "MySqlExtractor": "mysql",
    "BigQueryExtractor": ["gcpbigquery", "google_cloud_platform"],
    "SnowflakeExtractor": "snowflake",
    "TrinoExtractor": "trino",
    "AthenaExtractor": "aws",
    "SFTPExtractor": ["sftp", "ssh"],
    "FTPExtractor": "ftp",
}


class Extractors:
    """
    This exposes implemented extractors, while hiding ones that require additional, unmet
    dependency. Patchers are a category of extractor that needs to hook up to operator's
    internals during DAG creation.
    """

    def __init__(self):
        # Do not expose extractors relying on external dependencies that are not installed
        self.extractors = {}
        self.default_extractor = DefaultExtractor

        for extractor in _extractors:
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

        # Comma-separated extractors in OPENLINEAGE_EXTRACTORS variable.
        # Extractors should implement BaseExtractor
        env_extractors = os.getenv("OPENLINEAGE_EXTRACTORS")
        if env_extractors is not None:
            for extractor in env_extractors.split(";"):
                extractor = import_from_string(extractor.strip())
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

        def method_exists(method_name):
            method = getattr(clazz, method_name, None)
            if method:
                return callable(method)

        if method_exists("get_openlineage_facets_on_start") or method_exists(
            "get_openlineage_facets_on_complete"
        ):
            return self.default_extractor
        return None

    def instantiate_abstract_extractors(self, task) -> None:
        # instantiate sql check extractors
        from airflow.hooks.base import BaseHook

        if task.__class__.__name__ in (
            "SQLCheckOperator",
            "SQLValueCheckOperator",
            "SQLThresholdCheckOperator",
            "SQLIntervalCheckOperator",
            "SQLColumnCheckOperator",
            "SQLTableCheckOperator",
            "BigQueryTableCheckOperator",
            "BigQueryColumnCheckOperator",
        ):
            for extractor in self.extractors.values():
                conn_type = _check_providers.get(extractor.__name__, "")
                task_conn_type = None
                if hasattr(task, "gcp_conn_id"):
                    task_conn_type = BaseHook.get_connection(task.gcp_conn_id).conn_type
                elif hasattr(task, "conn_id"):
                    task_conn_type = BaseHook.get_connection(task.conn_id).conn_type
                if task_conn_type in conn_type:
                    check_extractors = get_check_extractors(extractor)
                    for check_extractor in check_extractors:
                        for operator_class in check_extractor.get_operator_classnames():
                            self.extractors[operator_class] = check_extractor
                    else:
                        return
            else:
                raise ValueError(
                    "Extractor for the given task's conn_type (%s) does not exist.",
                    task_conn_type,
                )

        # instantiate sql execute query extractor
        if task.__class__.__name__ == "SQLExecuteQueryOperator":
            task_conn_type = BaseHook.get_connection(task.conn_id).conn_type
            extractor_name = sql_extractors.get(task_conn_type, None)
            extractor = list(filter(lambda x: x.__name__ == extractor_name, self.extractors.values()))
            if extractor:
                self.extractors["SQLExecuteQueryOperator"] = get_sql_execute_query_extractor(extractor[0])
        return
