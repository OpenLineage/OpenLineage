from airflow.plugins_manager import AirflowPlugin
from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version


# Provide empty plugin for older version
if parse_version(AIRFLOW_VERSION) < parse_version("2.3.0.dev0"):
    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
else:
    from openlineage.airflow import listener

    # Provide entrypoint airflow plugin that registers listener module
    class OpenLineagePlugin(AirflowPlugin):
        name = "OpenLineagePlugin"
        listeners = [listener]
