# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from airflow.plugins_manager import AirflowPlugin

# Provide empty plugin for older version
from openlineage.airflow.macros import lineage_parent_id, lineage_run_id
from openlineage.airflow.utils import is_airflow_version_enough


def _is_disabled():
    return os.getenv("OPENLINEAGE_DISABLED", None) in [True, 'true', "True"]


if not is_airflow_version_enough("2.3.0") or _is_disabled():      # type: ignore
    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
        macros = [lineage_run_id, lineage_parent_id]
else:
    from openlineage.airflow import listener

    # Provide entrypoint airflow plugin that registers listener module
    class OpenLineagePlugin(AirflowPlugin):     # type: ignore
        name = "OpenLineagePlugin"
        listeners = [listener]
        macros = [lineage_run_id, lineage_parent_id]
