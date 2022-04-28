# SPDX-License-Identifier: Apache-2.0
from typing import List, Dict

import attr
from airflow.version import version as AIRFLOW_VERSION
from openlineage.client.facet import BaseFacet

from openlineage.airflow import __version__ as OPENLINEAGE_AIRFLOW_VERSION


@attr.s
class AirflowVersionRunFacet(BaseFacet):
    operator: str = attr.ib()
    taskInfo: str = attr.ib()
    airflowVersion: str = attr.ib()
    openlineageAirflowVersion: str = attr.ib()

    @classmethod
    def from_task(cls, task):
        return cls(
            f'{task.__class__.__module__}.{task.__class__.__name__}',
            str(task.__dict__),
            AIRFLOW_VERSION,
            OPENLINEAGE_AIRFLOW_VERSION
        )


@attr.s
class AirflowRunArgsRunFacet(BaseFacet):
    externalTrigger: bool = attr.ib(default=False)


@attr.s
class UnknownOperatorInstance:
    """
    Describes an unknown operator - specifies the (class) name of the operator and its properties
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
