# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

# Provide empty plugin for older version
from openlineage.airflow.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_parent_id,
    lineage_run_id,
)
from packaging.version import Version

from airflow.plugins_manager import AirflowPlugin
from airflow.version import version as AIRFLOW_VERSION


def _is_disabled():
    try:
        # If the Airflow provider is installed, skip running the openlineage-airflow plugin.
        from airflow.providers.openlineage.plugins.openlineage import (  # noqa: F401
            OpenLineageProviderPlugin,
        )

        return True
    except ImportError:
        pass
    return os.getenv("OPENLINEAGE_DISABLED", "").lower() == "true"


if (
    Version(AIRFLOW_VERSION) < Version("2.5.0.dev0")  # type: ignore
    or Version(AIRFLOW_VERSION) >= Version("2.8.0.b1")  # type: ignore
    or _is_disabled()
):

    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
        macros = [lineage_run_id, lineage_parent_id, lineage_job_namespace, lineage_job_name]

else:
    from openlineage.airflow import listener

    # Provide entrypoint airflow plugin that registers listener module
    class OpenLineagePlugin(AirflowPlugin):  # type: ignore
        name = "OpenLineagePlugin"
        listeners = [listener]
        macros = [lineage_run_id, lineage_parent_id, lineage_job_namespace, lineage_job_name]
