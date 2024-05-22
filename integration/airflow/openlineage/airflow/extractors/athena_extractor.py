# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional
from urllib.parse import urlparse

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.client.facet_v2 import sql_job
from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta, SqlMeta, parse


class AthenaExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["AthenaOperator", "AWSAthenaOperator"]

    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {"sql": sql_job.SQLJobFacet(query=SqlExtractor._normalize_sql(self.operator.query))}

        sql_meta: Optional[SqlMeta] = parse(self.operator.query, "generic", None)
        inputs: List[Dataset] = (
            list(
                filter(
                    None,
                    [
                        self._get_inout_dataset(table.schema or self.operator.database, table.name)
                        for table in sql_meta.in_tables
                    ],
                )
            )
            if sql_meta and sql_meta.in_tables
            else []
        )

        # Athena can output query result to a new table with CTAS query.
        # cf. https://docs.aws.amazon.com/athena/latest/ug/ctas.html
        outputs: List[Dataset] = (
            list(
                filter(
                    None,
                    [
                        self._get_inout_dataset(table.schema or self.operator.database, table.name)
                        for table in sql_meta.out_tables
                    ],
                )
            )
            if sql_meta and sql_meta.out_tables
            else []
        )

        # In addition to CTAS query, it's also possible to specify output location on S3
        # with a mandatory parameter, OutputLocation in ResultConfiguration.
        # cf. https://docs.aws.amazon.com/athena/latest/APIReference/API_ResultConfiguration.html#athena-Type-ResultConfiguration-OutputLocation  # noqa: E501
        #
        # Depending on the query type and the external_location property in the CTAS query,
        # its behavior changes as follows:
        #
        # * Normal SELECT statement
        #   -> The result is put into output_location as files rather than a table.
        #
        # * CTAS statement without external_location (`CREATE TABLE ... AS SELECT ...`)
        #   -> The result is put into output_location as a table,
        #      that is, both metadata files and data files are in there.
        #
        # * CTAS statement with external_location
        #   (`CREATE TABLE ... WITH (external_location='s3://bucket/key') AS SELECT ...`)
        #   -> The result is output as a table, but metadata and data files are
        #      separated into output_location and external_location respectively.
        #
        # For the last case, output_location may be unnecessary as OL's output information,
        # but we keep it as of now since it may be useful for some purpose.
        output_location = self.operator.output_location
        parsed = urlparse(output_location)
        outputs.append(
            Dataset(
                name=parsed.path or "/",
                source=Source(
                    scheme=parsed.scheme,
                    authority=parsed.netloc,
                    connection_url=output_location,
                ),
            )
        )

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets={},
            job_facets=job_facets,
        )

    def _get_inout_dataset(self, database, table) -> Optional[Dataset]:
        # Currently, AthenaOperator and AthenaHook don't have a functionality to specify catalog,
        # and it seems to implicitly assume that the default catalog (AwsDataCatalog) is target.
        CATALOG_NAME = "AwsDataCatalog"

        # AthenaHook.get_conn() doesn't return PEP-249 compliant connection object,
        # but Boto3's Athena.Client instead. So this class doesn't inherit from
        # SqlExtractor, which depends on PEP-249 compliant connection.
        client = self.operator.hook.get_conn()
        try:
            table_metadata = client.get_table_metadata(
                CatalogName=CATALOG_NAME, DatabaseName=database, TableName=table
            )
            s3_location = table_metadata["TableMetadata"]["Parameters"]["location"]
            table_schema = DbTableSchema(
                schema_name=database,
                table_name=DbTableMeta(f"{CATALOG_NAME}.{database}.{table}"),
                columns=[
                    DbColumn(name=column["Name"], type=column["Type"], ordinal_position=i)
                    for i, column in enumerate(table_metadata["TableMetadata"]["Columns"])
                ],
            )

            scheme = "awsathena"
            authority = f"athena.{client._client_config.region_name}.amazonaws.com"

            return Dataset.from_table_schema(
                source=Source(
                    scheme=scheme,
                    authority=authority,
                    connection_url=f"{scheme}://{authority}",
                ),
                table_schema=table_schema,
                database_name=CATALOG_NAME,
                data_location=s3_location,
            )

        except Exception as e:
            self.log.error(f"Cannot retrieve table metadata from Athena.Client. {e}", exc_info=True)
            return None
