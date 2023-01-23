# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, List

import attr
from openlineage.airflow.version import __version__ as OPENLINEAGE_AIRFLOW_VERSION
from openlineage.client.facet import BaseFacet
from openlineage.client.utils import RedactMixin

from airflow.version import version as AIRFLOW_VERSION


@attr.s
class AirflowVersionRunFacet(BaseFacet):
    operator: str = attr.ib()
    taskInfo: Dict[str, object] = attr.ib()
    airflowVersion: str = attr.ib()
    openlineageAirflowVersion: str = attr.ib()

    _additional_skip_redact: List[str] = [
        "operator",
        "airflowVersion",
        "openlineageAirflowVersion",
    ]

    @classmethod
    def from_dagrun_and_task(cls, dagrun, task):
        # task.__dict__ may contain values uncastable to str
        from openlineage.airflow.utils import get_operator_class, to_json_encodable

        task_info = to_json_encodable(task)
        task_info["dag_run"] = to_json_encodable(dagrun)

        return cls(
            operator=f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
            taskInfo=task_info,
            airflowVersion=AIRFLOW_VERSION,
            openlineageAirflowVersion=OPENLINEAGE_AIRFLOW_VERSION,
        )


@attr.s
class AirflowRunArgsRunFacet(BaseFacet):
    externalTrigger: bool = attr.ib(default=False)

    _additional_skip_redact: List[str] = ["externalTrigger"]


@attr.s
class AirflowMappedTaskRunFacet(BaseFacet):
    mapIndex: int = attr.ib()
    operatorClass: str = attr.ib()

    _additional_skip_redact: List[str] = ["operatorClass"]

    @classmethod
    def from_task_instance(cls, task_instance):
        task = task_instance.task
        from openlineage.airflow.utils import get_operator_class

        return cls(
            task_instance.map_index,
            f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
        )


@attr.s
class AirflowRunFacet(BaseFacet):
    """
    Composite Airflow run facet.
    """
    dag: Dict = attr.ib()
    dagRun: Dict = attr.ib()
    task: Dict = attr.ib()
    taskInstance: Dict = attr.ib()
    taskUuid: str = attr.ib()


@attr.s
class UnknownOperatorInstance(RedactMixin):
    """
    Describes an unknown operator - specifies the (class) name of the operator
    and its properties
    """

    name: str = attr.ib()
    properties: Dict[str, object] = attr.ib()
    type: str = attr.ib(default="operator")

    _skip_redact: List[str] = ["name", "type"]


@attr.s
class UnknownOperatorAttributeRunFacet(BaseFacet):
    """
    RunFacet that describes unknown operators in an Airflow DAG
    """

    unknownItems: List[UnknownOperatorInstance] = attr.ib()
