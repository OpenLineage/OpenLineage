# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import attr
from openlineage.airflow.utils import LoggingMixin, get_job_name
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Dataset


@attr.s
class OperatorLineage:
    inputs: List[Dataset] = attr.ib(factory=list)
    outputs: List[Dataset] = attr.ib(factory=list)
    run_facets: Dict[str, BaseFacet] = attr.ib(factory=dict)
    job_facets: Dict[str, BaseFacet] = attr.ib(factory=dict)


@attr.s
class TaskMetadata:
    name: str = attr.ib()  # deprecated
    inputs: List[Dataset] = attr.ib(factory=list)
    outputs: List[Dataset] = attr.ib(factory=list)
    run_facets: Dict[str, BaseFacet] = attr.ib(factory=dict)
    job_facets: Dict[str, BaseFacet] = attr.ib(factory=dict)


class BaseExtractor(ABC, LoggingMixin):

    _whitelist_query_params: List[str] = []

    def __init__(self, operator):
        super().__init__()
        self.operator = operator
        self.patch()

    def patch(self):
        # Extractor should register extension methods or patches to operator here
        pass

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        """
        Implement this method returning list of operators that extractor works for.
        Particularly, in Airflow 2 some operators are deprecated and simply subclass the new
        implementation, for example BigQueryOperator:
        https://github.com/apache/airflow/blob/main/airflow/contrib/operators/bigquery_operator.py
        The BigQueryExtractor needs to work with both of them.
        :return:
        """
        raise NotImplementedError()

    def validate(self):
        assert self.operator.__class__.__name__ in self.get_operator_classnames()

    @abstractmethod
    def extract(self) -> Optional[TaskMetadata]:
        pass

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        return self.extract()

    @classmethod
    def get_connection_uri(cls, conn):
        """
        Return the connection URI for the given ID. We first attempt to lookup
        the connection URI via AIRFLOW_CONN_<conn_id>, else fallback on querying
        the Airflow's connection table.
        """

        conn_uri = conn.get_uri()
        parsed = urlparse(conn_uri)

        # Remove username and password
        netloc = f"{parsed.hostname}" + (f":{parsed.port}" if parsed.port else "")
        parsed = parsed._replace(netloc=netloc)
        if parsed.query:
            query_dict = dict(parse_qsl(parsed.query))
            if conn.EXTRA_KEY in query_dict:
                query_dict = json.loads(query_dict[conn.EXTRA_KEY])
            filtered_qs = {
                k: v for k, v in query_dict.items() if k in cls._whitelist_query_params
            }
            parsed = parsed._replace(query=urlencode(filtered_qs))
        return urlunparse(parsed)


class DefaultExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        """
        Default extractor is chosen not on the classname basis, but
        by existence of get_openlineage_facets method on operator
        """
        return []

    def extract(self) -> Optional[TaskMetadata]:
        try:
            return self._get_openlineage_facets(
                self.operator.get_openlineage_facets_on_start
            )
        except AttributeError:
            return None

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        on_complete = getattr(self.operator, 'get_openlineage_facets_on_complete', None)
        if on_complete and callable(on_complete):
            return self._get_openlineage_facets(
                on_complete, task_instance
            )
        return self.extract()

    def _get_openlineage_facets(
        self, get_facets_method, *args
    ) -> Optional[TaskMetadata]:
        facets: OperatorLineage = get_facets_method(*args)
        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=facets.inputs,
            outputs=facets.outputs,
            run_facets=facets.run_facets,
            job_facets=facets.job_facets,
        )
