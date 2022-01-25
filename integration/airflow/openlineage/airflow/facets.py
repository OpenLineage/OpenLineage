# SPDX-License-Identifier: Apache-2.0

import attr
from airflow.version import version as AIRFLOW_VERSION
from openlineage.client.facet import BaseFacet

from openlineage.airflow import __version__ as OPENLINEAGE_AIRFLOW_VERSION
from openlineage.common.schema.unknown_source import UnknownSourceAttributeRunFacet


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
