# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from openlineage.airflow.macros import lineage_parent_id, lineage_run_id
from openlineage.airflow.utils import is_airflow_version_between, is_airflow_version_enough

from airflow.plugins_manager import AirflowPlugin


def _is_disabled():
    return os.getenv("OPENLINEAGE_DISABLED", None) in [True, 'true', "True"]


if not is_airflow_version_enough("2.3.0") or _is_disabled():  # type: ignore
    # Provide empty plugin for older version
    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
        macros = [lineage_run_id, lineage_parent_id]
elif is_airflow_version_between("2.3.0", "2.4.0"):
    from openlineage.airflow import listener

    # Provide entrypoint airflow plugin that registers listener module
    class OpenLineagePlugin(AirflowPlugin):  # type: ignore
        name = "OpenLineagePlugin"
        macros = [lineage_run_id, lineage_parent_id]
        listeners = [listener]

else:
    from openlineage.airflow.listener_executor import ListenerPlugin

    # This listener is fully executor-based and ment to work with Airflow versions that support
    # event mechanism without sqlalchemy-based events
    class OpenLineagePlugin(AirflowPlugin):  # type: ignore
        name = "OpenLineagePlugin"
        macros = [lineage_run_id, lineage_parent_id]
        listeners = [ListenerPlugin()]
