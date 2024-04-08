# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

import attr
from openlineage.client.facet import BaseFacet


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
