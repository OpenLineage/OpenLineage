# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from typing import List, Dict, Optional

import attr
from airflow.version import version as AIRFLOW_VERSION
from openlineage.client.facet import BaseFacet

from openlineage.airflow import __version__ as OPENLINEAGE_AIRFLOW_VERSION

from openlineage.integration.common.openlineage.schema import GITHUB_LOCATION


@attr.s
class AirflowVersionRunFacet(BaseFacet):
    operator: str = attr.ib()
    taskInfo: str = attr.ib()
    airflowVersion: str = attr.ib()
    openlineageAirflowVersion: str = attr.ib()

    @classmethod
    def from_task(cls, task):
        # task.__dict__ may contain values uncastable to str
        from openlineage.airflow.utils import SafeStrDict

        return cls(
            f"{task.__class__.__module__}.{task.__class__.__name__}",
            str(SafeStrDict(task.__dict__)),
            AIRFLOW_VERSION,
            OPENLINEAGE_AIRFLOW_VERSION,
        )


@attr.s
class AirflowRunArgsRunFacet(BaseFacet):
    externalTrigger: bool = attr.ib(default=False)


@attr.s
class UnknownOperatorInstance:
    """
    Describes an unknown operator - specifies the (class) name of the operator
    and its properties
    """

    name: str = attr.ib()
    properties: Dict[str, object] = attr.ib()
    type: str = attr.ib(default="operator")


@attr.s
class UnknownOperatorAttributeRunFacet(BaseFacet):
    """
    RunFacet that describes unknown operators in an Airflow DAG
    """

    unknownItems: List[UnknownOperatorInstance] = attr.ib()


@attr.s
class PythonDecoratedFacet(BaseFacet):
    """
    Facet that represents metadata relevant to LiveMaps Graph UI.
    :param database: The database type/name
    :param cluster: Cluster of the cloud database(Optional)
    :param connectionUrl: Database connection URL
    :param target: Target of Link/outEdge for LiveMaps Graph UI.
    :param source: Source of link/inEdge for LiveMaps Graph UI.
    """

    database: str = attr.ib(default=None)
    cluster: Optional[str] = attr.ib(default=None)
    connectionUrl: str = attr.ib(default=None)
    target: Optional[str] = attr.ib(default=None)
    source: Optional[str] = attr.ib(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "python-decorated-facet.json"
