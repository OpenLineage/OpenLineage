# SPDX-License-Identifier: Apache-2.0.
import logging
from typing import List, Optional, TYPE_CHECKING, Dict, Tuple
from urllib.parse import urlparse

from openlineage.airflow.extractors.dbapi_utils import get_table_schemas
from openlineage.airflow.utils import get_connection
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.client.facet import BaseFacet, SqlJobFacet
from openlineage.common.sql import SqlMeta, parse
from openlineage.common.dataset import Dataset, Source
from abc import abstractmethod

if TYPE_CHECKING:
    from airflow.models import Connection, BaseHook

logger = logging.getLogger(__name__)


class SqlExtractor(BaseExtractor):
    default_schema = "public"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._conn = None
        self._hook = None

    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {"sql": SqlJobFacet(self.operator.sql)}
        run_facets: Dict = {}

        # (1) Parse sql statement to obtain input / output tables.
        logger.debug("Sending SQL to parser")
        sql_meta: Optional[SqlMeta] = parse(self.operator.sql, self.default_schema)
        logger.debug(f"Got meta {sql_meta}")

        if not sql_meta:
            return TaskMetadata(
                name=task_name,
                inputs=[],
                outputs=[],
                run_facets=run_facets,
                job_facets=job_facets,
            )

        # (2) Construct source object
        source = Source(
            scheme=self._scheme,
            authority=self._get_authority(),
            connection_url=self._get_connection_uri(),
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
            self._get_in_query(sql_meta.in_tables) if sql_meta.in_tables else None,
            self._get_out_query(sql_meta.out_tables) if sql_meta.out_tables else None,
        )

        for ds in inputs:
            ds.input_facets = self._get_input_facets()

        for ds in outputs:
            ds.output_facets = self._get_output_facets()

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
        return getattr(self.hook, self.hook.conn_name_attr)

    @property
    def hook(self):
        if not self._hook:
            self._hook = self._get_hook()
        return self._hook

    @property
    def conn(self) -> "Connection":
        if not self._conn:
            self._conn = get_connection(self._conn_id())
        return self._conn

    @property
    def _scheme(self) -> str:
        return self._get_scheme()

    @abstractmethod
    def _get_scheme(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get_database(self) -> str:
        raise NotImplementedError

    def _get_authority(self) -> str:
        if self.conn.host and self.conn.port:
            return f"{self.conn.host}:{self.conn.port}"
        else:
            parsed = urlparse(self.conn.get_uri())
            return f"{parsed.hostname}:{parsed.port}"

    @abstractmethod
    def _get_hook(self) -> "BaseHook":
        raise NotImplementedError

    @abstractmethod
    def _get_connection_uri(self) -> str:
        raise NotImplementedError

    # optional to implement
    def _get_in_query(self, in_tables) -> str:
        pass

    # optional to implement
    def _get_out_query(self, out_tables) -> str:
        pass

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
