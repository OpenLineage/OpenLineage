# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import ClassVar, Dict, List

import attr
from openlineage.airflow.version import __version__ as OPENLINEAGE_AIRFLOW_VERSION
from openlineage.client.facet_v2 import BaseFacet
from openlineage.client.utils import RedactMixin

from airflow.version import version as AIRFLOW_VERSION


@attr.define
class AirflowVersionRunFacet(BaseFacet):
    operator: str
    taskInfo: Dict[str, object]
    airflowVersion: str
    openlineageAirflowVersion: str

    _additional_skip_redact: ClassVar[List[str]] = [
        "operator",
        "airflowVersion",
        "openlineageAirflowVersion",
    ]

    @classmethod
    def from_dagrun_and_task(cls, dagrun, task):
        from openlineage.airflow.utils import get_operator_class

        return cls(
            operator=f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
            taskInfo={},
            airflowVersion=AIRFLOW_VERSION,
            openlineageAirflowVersion=OPENLINEAGE_AIRFLOW_VERSION,
        )


@attr.define
class AirflowRunArgsRunFacet(BaseFacet):
    externalTrigger: bool = False

    _additional_skip_redact: ClassVar[List[str]] = ["externalTrigger"]


@attr.define
class AirflowMappedTaskRunFacet(BaseFacet):
    mapIndex: int
    operatorClass: str

    _additional_skip_redact: ClassVar[List[str]] = ["operatorClass"]

    @classmethod
    def from_task_instance(cls, task_instance):
        task = task_instance.task
        from openlineage.airflow.utils import get_operator_class

        return cls(
            task_instance.map_index,
            f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
        )


@attr.define
class AirflowRunFacet(BaseFacet):
    """
    Composite Airflow run facet.
    """

    dag: Dict
    dagRun: Dict
    task: Dict
    taskInstance: Dict
    taskUuid: str


@attr.define
class UnknownOperatorInstance(RedactMixin):
    """
    Describes an unknown operator - specifies the (class) name of the operator
    and its properties
    """

    name: str
    properties: Dict[str, object]
    type: str = "operator"

    _skip_redact: ClassVar[List[str]] = ["name", "type"]


@attr.define
class UnknownOperatorAttributeRunFacet(BaseFacet):
    """
    RunFacet that describes unknown operators in an Airflow DAG
    """

    unknownItems: List[UnknownOperatorInstance]
