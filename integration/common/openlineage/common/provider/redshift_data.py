# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import traceback
from typing import Any, Dict, List, Optional

import attr
import botocore
from openlineage.client.facet import (
    BaseFacet,
    ErrorMessageRunFacet,
    OutputStatisticsOutputDatasetFacet,
)
from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta


@attr.s
class RedshiftFacets:
    run_facets: Dict[str, BaseFacet] = attr.ib()
    inputs: List[Dataset] = attr.ib()
    output: List[Dataset] = attr.ib()


class RedshiftDataDatasetsProvider:
    def __init__(
        self,
        client: botocore.client,
        connection_details: Dict[str, Any],
        logger: Optional[logging.Logger] = None,
    ):
        if logger is None:
            self.logger: logging.Logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.connection_details = connection_details
        self.client = client

    def get_facets(
        self, job_id: str, inputs: List[DbTableMeta], outputs: List[DbTableMeta]
    ) -> RedshiftFacets:
        ds_inputs = []
        ds_outputs = []
        run_facets: Dict[str, BaseFacet] = {}
        dataset_stat_facet = None

        source = Source(
            scheme="redshift",
            authority=self._get_authority(),
        )
        try:
            properties = self.client.describe_statement(Id=job_id)
            dataset_stat_facet = self._get_output_statistics(properties)
        except Exception as e:
            self.logger.error(
                f"Cannot retrieve job details from Redshift Data client. {e}",
                exc_info=True,
            )
            run_facets.update(
                {
                    "errorMessage": ErrorMessageRunFacet(
                        message=f"Cannot retrieve job details from Redshift Data client. {e}",
                        programmingLanguage="PYTHON",
                        stackTrace=f"{e}: {traceback.format_exc()}",
                    )
                }
            )

        ds_inputs = self._get_dataset_from_tables(inputs, source)
        ds_outputs = self._get_dataset_from_tables(outputs, source)

        for ds_output in ds_outputs:
            ds_output.custom_facets.update({
                        "stats": dataset_stat_facet
                    })

        return RedshiftFacets(run_facets, ds_inputs, ds_outputs)

    def _get_output_statistics(self, properties) -> OutputStatisticsOutputDatasetFacet:
        return OutputStatisticsOutputDatasetFacet(
            rowCount=properties.get("ResultRows"),
            size=properties.get("ResultSize"),
        )

    def _get_dataset_from_tables(
        self, tables: List[DbTableMeta], source: Source
    ) -> Dataset:
        try:
            return [
                Dataset.from_table_schema(
                    source=source,
                    table_schema=table_schema,
                    database_name=self.connection_details.get("database"),
                )
                for table_schema in self._get_table_schemas(tables)
            ]
        except Exception as e:
            self.logger.warning(f"Could not extract schema from redshift. {e}")
            return [Dataset.from_table(source, table.name) for table in tables]

    def _get_authority(self) -> str:
        return (
            f"{self.connection_details['cluster_identifier']}."
            f"{self.connection_details.get('region')}:5439"
        )

    def _get_table_safely(self, output_table_name):
        try:
            return self._get_table(output_table_name)
        except Exception as e:
            self.logger.warning(f"Could not extract output schema from redshift. {e}")
        return None

    def _get_table_schemas(self, tables: List[DbTableMeta]) -> List[DbTableSchema]:
        if not tables:
            return []
        return [self._get_table(table) for table in tables]

    def _get_table(self, table: DbTableMeta) -> Optional[DbTableSchema]:
        kwargs: Dict[str, Any] = {
            "ClusterIdentifier": self.connection_details.get("cluster_identifier"),
            "Database": table.database or self.connection_details.get("database"),
            "Table": table.name,
            "DbUser": self.connection_details.get("db_user"),
            "SecretArn": self.connection_details.get("secret_arn"),
            "Schema": table.schema,
        }
        filter_values = {key: val for key, val in kwargs.items() if val is not None}
        redshift_table = self.client.describe_table(**filter_values)
        fields = redshift_table.get("ColumnList")
        if not fields:
            return None
        schema_name = fields[0]["schemaName"]
        columns = [
            DbColumn(
                name=fields[i].get("name"),
                type=fields[i].get("typeName"),
                ordinal_position=i,
            )
            for i in range(len(fields))
        ]

        return DbTableSchema(
            schema_name=schema_name,
            table_name=DbTableMeta(redshift_table.get("TableName")),
            columns=columns,
        )
