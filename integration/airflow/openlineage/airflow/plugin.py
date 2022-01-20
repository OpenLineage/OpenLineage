from airflow.plugins_manager import AirflowPlugin
from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version

from openlineage.airflow import listener

# Provide empty plugin for older version
if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
else:
    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
        listeners = [listener]
