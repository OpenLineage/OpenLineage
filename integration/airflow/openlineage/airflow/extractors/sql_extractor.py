# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from abc import abstractmethod
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.dbapi_utils import (
    TablesHierarchy,
    create_information_schema_query,
    get_table_schemas,
)
from openlineage.airflow.utils import get_connection
from openlineage.client.facet import (
    BaseFacet,
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    ExtractionError,
    ExtractionErrorRunFacet,
    SqlJobFacet,
)
from openlineage.common.dataset import Dataset, Source
from openlineage.common.sql import DbTableMeta, SqlMeta, parse

if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook
    from airflow.models import Connection


class SqlExtractor(BaseExtractor):
    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "udt_name",
    ]
    _information_schema_table_name = "information_schema.columns"
    _is_information_schema_cross_db = False
    _is_uppercase_names = False
    _allow_trailing_semicolon = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._conn = None
        self._hook = None
        self._database: Optional[str] = None
        self._scheme: Optional[str] = None

    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {"sql": SqlJobFacet(query=self._normalize_sql(self.operator.sql))}
        run_facets: Dict = {}

        # (1) Parse sql statement to obtain input / output tables.
        self.log.debug(f"Sending SQL to parser: {self.operator.sql}")
        sql_meta: Optional[SqlMeta] = parse(
            self.operator.sql,
            dialect=self.dialect,
            default_schema=self.default_schema
        )
        self.log.debug(f"Got meta {sql_meta}")

        if not sql_meta:
            return TaskMetadata(
                name=task_name,
                inputs=[],
                outputs=[],
                run_facets=run_facets,
                job_facets=job_facets,
            )

        if sql_meta.errors:
            run_facets['extractionError'] = ExtractionErrorRunFacet(
                totalTasks=len(self.operator.sql) if isinstance(self.operator.sql, list) else 1,
                failedTasks=len(sql_meta.errors),
                errors=[
                    ExtractionError(
                        errorMessage=error.message,
                        stackTrace=None,
                        task=error.origin_statement,
                        taskNumber=error.index
                    )
                    for error
                    in sql_meta.errors
                ]
            )

        # (2) Construct source object
        source = Source(
            scheme=self.scheme,
            authority=self._get_authority(),
            connection_url=self.get_connection_uri(self.conn),
        )

        database = getattr(self.operator, "database", None)
        if not database:
            database = self._get_database()

        # (3) Map input / output tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs, outputs = get_table_schemas(
            self.hook,
            source,
            database,
            self._information_schema_query(sql_meta.in_tables)
            if sql_meta.in_tables
            else None,
            self._information_schema_query(sql_meta.out_tables)
            if sql_meta.out_tables
            else None,
        )

        for ds in inputs:
            ds.input_facets = self._get_input_facets()

        for ds in outputs:
            ds.output_facets = self._get_output_facets()
            if len(outputs) == 1:  # Should be always true
                self.attach_column_facet(ds, sql_meta)

        db_specific_run_facets = self._get_db_specific_run_facets(
            source, inputs, outputs
        )

        run_facets = {**run_facets, **db_specific_run_facets}

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def _conn_id(self) -> str:
        if hasattr(self.hook, "conn_id"):
            return "conn_id"
        return getattr(self.hook, self.hook.conn_name_attr)

    @property
    def dialect(self):
        return "generic"

    @property
    def default_schema(self):
        return "public"

    @property
    def hook(self):
        if not self._hook:
            self._hook = SqlExtractor._get_hook(self) or self._get_hook()
        return self._hook

    @property
    def conn(self) -> "Connection":
        if not self._conn:
            self._conn = get_connection(self._conn_id())
        return self._conn  # type: ignore

    @property
    def scheme(self) -> Optional[str]:
        if not self._scheme:
            self._scheme = self._get_scheme()
        return self._scheme

    @property
    def database(self) -> Optional[str]:
        if not self._database:
            self._database = self._get_database()
        return self._database

    @abstractmethod
    def _get_scheme(self) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def _get_database(self) -> Optional[str]:
        raise NotImplementedError

    def _get_authority(self) -> str:
        if self.conn.host and self.conn.port:
            return f"{self.conn.host}:{self.conn.port}"
        else:
            parsed = urlparse(self.conn.get_uri())
            return f"{parsed.hostname}:{parsed.port}"

    def _get_hook(self) -> "Optional[BaseHook]":
        return getattr(self.operator, "_hook", None)

    def _get_db_specific_run_facets(
        self,
        source: Source,
        inputs: Tuple[List[Dataset], ...],
        outputs: Tuple[List[Dataset], ...],
    ) -> Dict[str, BaseFacet]:
        return {}

    def _get_input_facets(self) -> Dict[str, BaseFacet]:
        return {}

    def _get_output_facets(self) -> Dict[str, BaseFacet]:
        return {}

    @staticmethod
    def _normalize_name(name: str) -> str:
        return name.lower()

    def attach_column_facet(self, dataset, sql_meta: SqlMeta):
        if not len(sql_meta.column_lineage):
            return
        dataset.custom_facets['columnLineage'] = ColumnLineageDatasetFacet(
            fields={
                column_lineage.descendant.name: ColumnLineageDatasetFacetFieldsAdditional(
                    inputFields=[
                        ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                            namespace=dataset.source.name,
                            name=column_meta.origin.qualified_name if column_meta.origin else "",
                            field=column_meta.name
                        ) for column_meta in column_lineage.lineage
                    ],
                    transformationType="",
                    transformationDescription=""
                ) for column_lineage in sql_meta.column_lineage

            }
        )

    def _information_schema_query(self, tables: List[DbTableMeta]) -> str:
        tables_hierarchy = self._get_tables_hierarchy(
            tables,
            normalize_name=self._normalize_name,
            database=self.database,
            is_cross_db=self._is_information_schema_cross_db,
        )
        return create_information_schema_query(
            columns=self._information_schema_columns,
            information_schema_table_name=self._information_schema_table_name,
            tables_hierarchy=tables_hierarchy,
            uppercase_names=self._is_uppercase_names,
            allow_trailing_semicolon=self._allow_trailing_semicolon,
        )

    @staticmethod
    def _normalize_sql(sql: Union[str, Iterable[str]]):
        if isinstance(sql, str):
            sql = [stmt for stmt in sql.split(";") if stmt != ""]
        sql = [obj for stmt in sql for obj in stmt.split(";") if obj != ""]
        return ";\n".join(sql)

    @staticmethod
    def _get_tables_hierarchy(
        tables: List[DbTableMeta],
        normalize_name: Callable[[str], str],
        database: Optional[str] = None,
        is_cross_db: bool = False,
    ) -> TablesHierarchy:
        hierarchy: TablesHierarchy = {}
        for table in tables:
            if is_cross_db:
                db = table.database or database
            else:
                db = None
            hierarchy.setdefault(
                normalize_name(db) if db else db, {}
            ).setdefault(
                normalize_name(table.schema) if table.schema else db, []
            ).append(table.name)
        return hierarchy
