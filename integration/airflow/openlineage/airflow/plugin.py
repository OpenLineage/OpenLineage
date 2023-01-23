# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

# Provide empty plugin for older version
from openlineage.airflow.macros import lineage_parent_id, lineage_run_id
from pkg_resources import parse_version

from airflow.plugins_manager import AirflowPlugin
from airflow.version import version as AIRFLOW_VERSION


def _is_disabled():
    return os.getenv("OPENLINEAGE_DISABLED", None) in [True, 'true', "True"]


if parse_version(AIRFLOW_VERSION) \
        < parse_version("2.3.0.dev0") or _is_disabled():      # type: ignore
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
