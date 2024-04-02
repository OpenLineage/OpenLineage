from __future__ import annotations

import os
import re
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import __version__ as airflow_version
from distutils.version import StrictVersion
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path


BYPASS_LATEST_VERSION_CHECK = False  # set this to True to bypass the latest version check for openlineage-airflow library and OpenLineage provider package. Version check will be skipped if unable to access PyPI URL
LINEAGE_BACKEND = "MARQUEZ"  # update this if using any other backend for OpenLineage events ingestion and implement custom checks in verify_custom_http_backend function

log = logging.getLogger(__name__)


def _extract_version_from_string(version_string: str) -> str | None:
    match = re.match(r'^(\d+\.\d+\.\d+)', version_string)
    return match.group(1) if match else None


def _get_latest_package_version(library_name: str) -> str | None:
    try:
        import requests
        response = requests.get(f"https://pypi.org/pypi/{library_name}/json")
        response.raise_for_status()
        version_string = response.json()['info']['version']
        return _extract_version_from_string(version_string)
    except Exception as e:
        log.error(f"Failed to fetch latest version for {library_name} from PyPI: {e}")
        return None


def _get_installed_package_version(library_name):
    try:
        version_string = _extract_version_from_string(version(library_name))
        return StrictVersion(version_string) if version_string else None
    except PackageNotFoundError:
        return None


def _provider_can_be_used() -> bool:
    try:
        version_string = _extract_version_from_string(airflow_version)
        parsed_version = StrictVersion(version_string)
    except Exception:
        raise ValueError(f"Failed to parse airflow version: {airflow_version}")
    if parsed_version < StrictVersion('2.1'):
        raise RuntimeError('OpenLineage is not supported in Airflow versions <2.1')
    elif parsed_version >= StrictVersion('2.7'):
        return True
    return False


def is_ol_installed():
    library_name = "openlineage-airflow"
    if _provider_can_be_used():
        library_name = "apache-airflow-providers-openlineage"

    library_version = _get_installed_package_version(library_name)
    if not library_version:
        raise ValueError(f"{library_name} not installed")
    if BYPASS_LATEST_VERSION_CHECK:
        log.info(f"Bypassing the latest version check for {library_name}")
        return
    latest_version = _get_latest_package_version(library_name)
    if latest_version is None:
        log.warning(f"Failed to fetch the latest version for {library_name}. Skipping version check.")
        return

    if library_version < latest_version:
        raise ValueError(
            f"{library_name} is out of date. "
            f"Installed version: {library_version}, Required version: {latest_version}"
        )

def _check_transport_env_vars() -> bool:
    from airflow.configuration import conf as airflow_conf
    transport = os.getenv("AIRFLOW__OPENLINEAGE__TRANSPORT", False) or airflow_conf.get("openlineage", "transport",
                                                                                        fallback=False)
    if transport is False:
        log.info(
            "Airflow OL transport is not set using AIRFLOW__OPENLINEAGE__TRANSPORT, now checking OPENLINEAGE_CONFIG")
    else:  # print the transport and ask the user to check https://openlineage.io/docs/client/python/#built-in-transport-types, format should match.
        log.error(
            "Transport value: {}\nPlease check the format at https://openlineage.io/docs/client/python/#built-in-transport-types".format(
                transport))
        return True


def _check_openlineage_config(env_var_name) -> bool:
    if env_var_name == "AIRFLOW__OPENLINEAGE__CONFIG_PATH":
        from airflow.configuration import conf as airflow_conf
        config_path = os.getenv(env_var_name, False) or airflow_conf.get("openlineage", "config_path", fallback=False)
    else:
        config_path = os.getenv(env_var_name, False)
    if config_path is "" or config_path is False:
        log.info("OL transport is not set using {}".format(env_var_name))
    else:
        config_file = Path(config_path)
        if config_file.is_file():
            if os.path.getsize(config_path) == 0:
                log.error("Config file is empty at path: {}".format(config_path))
        else:
            log.error("File does not exist at path: {}".format(config_path))
        return True

def _check_openlineage_yml(file_path) -> bool:
    if os.path.exists(os.path.expanduser(file_path)):
        if os.path.getsize(os.path.expanduser(file_path)) == 0:
            log.error("{} is empty".format(file_path))
            return True
    else:
        log.info("{} is not present.".format(file_path))

def _check_http_env_vars() -> bool:
    # check for env variables for HTTP transport - OPENLINEAGE_URL
    if not os.getenv("OPENLINEAGE_URL", False):
        log.info(
            "OPENLINEAGE_URL is not set. Please set up OpenLineage using documentation at "
            "https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html")
    else:
        log.error("OPENLINEAGE_URL is set to: {}".format(os.getenv("OPENLINEAGE_URL")))
        return True


def _debug_missing_transport():
    # Here we check all the variables, conf etc. and debug why the transport is not set properly
    if _provider_can_be_used():
        if _check_openlineage_config("AIRFLOW__OPENLINEAGE__CONFIG_PATH"): return True
        if _check_transport_env_vars(): return True

    if _check_openlineage_config("OPENLINEAGE_CONFIG"): return True
    if _check_openlineage_yml("openlineage.yml"): return True
    if _check_openlineage_yml("~/.openlineage/openlineage.yml"): return True
    if _check_http_env_vars(): return True
    raise Exception("OpenLineage transport is not set properly, please refer to the OL setup docs.")


def is_ol_accessible_and_enabled():
    if _provider_can_be_used():
        try:
            from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
            from airflow.providers.openlineage.plugins.openlineage import _is_disabled
            from airflow.configuration import conf as airflow_conf
            if _is_disabled():
                if airflow_conf.getboolean("openlineage", "disabled", fallback=False):
                    raise ValueError("OpenLineage is disabled in airflow.cfg: openlineage.disabled")
                elif os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true":
                    raise ValueError("OpenLineage is disabled due to the environment variable OPENLINEAGE_DISABLED")
                raise ValueError(
                    "OpenLineage is disabled because required config/env variables are not set. "
                    "Please refer to "
                    "https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html"
                )
            try:
                transport = get_openlineage_listener().adapter.get_or_create_openlineage_client().transport
            except Exception:
                transport = None
            if transport is None or transport.kind == "console":
                if _debug_missing_transport():
                    raise Exception("OpenLineage config is not set up properly. Please check the log above.")
        except ImportError or ModuleNotFoundError as e:
            raise ValueError("provider package not installed", e)
    else:
        try:
            from openlineage.airflow.plugin import _is_disabled, OpenLineagePlugin
            if _is_disabled():
                if os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true":
                    raise Exception(
                        "OpenLineage is disabled due to the environment variable OPENLINEAGE_DISABLED")
                raise Exception(
                    "OpenLineage is disabled because required config/env variables are not set."
                    " Please refer to https://openlineage.io/docs/client/python"
                )
            try:
                transport = OpenLineagePlugin.listeners[0].adapter.get_or_create_openlineage_client().transport
            except Exception:
                transport = None
            if transport is None or transport.kind == "console":
                if _debug_missing_transport():
                    raise Exception("OpenLineage config is not set up properly. Please check the log above.")
        except ImportError or ModuleNotFoundError as e:
            raise ValueError("OL package not installed", e)


def validate_connection():
    if _provider_can_be_used():
        from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
        transport = get_openlineage_listener().adapter.get_or_create_openlineage_client().transport
    else:
        from openlineage.airflow.plugin import OpenLineagePlugin
        transport = OpenLineagePlugin.listeners[0].adapter.get_or_create_openlineage_client().transport

    config = extract_config(transport)
    verify_backend(LINEAGE_BACKEND, config)


def extract_config(transport):
    if transport.kind == 'http':
        return _extract_http_config(transport)
    elif transport.kind == 'kafka':
        return 'extract_kafka_config'
    elif transport.kind == 'file':
        return 'extract_file_config'
    raise ValueError("Unsupported transport type: ", transport.kind)


def _extract_http_config(transport):
    config = {
        "url": transport.config.url,
        "endpoint": transport.config.endpoint,
        "auth": transport.config.auth,
    }
    return config


def _extract_kafka_config(transport):
    raise NotImplementedError("This feature is not implemented yet")


def _extract_file_config(transport):
    raise NotImplementedError("This feature is not implemented yet")


def verify_backend(backend_type: str, config: dict):
    backend_type = backend_type.lower()
    if backend_type == 'marquez':
        return _verify_marquez_http_backend(config)
    elif backend_type == 'atlan':
        return _verify_atlan_http_backend(config)
    elif backend_type == 'marquez':
        return _verify_custom_backend(config)
    raise ValueError(f"Unsupported backend type: {backend_type}")


def _verify_marquez_http_backend(config):
    log.info("Checking Marquez setup")
    ol_url = config['url']
    ol_endpoint = config['endpoint']  # "api/v1/lineage"
    marquez_prefix_path = ol_endpoint[:ol_endpoint.rfind('/')+1] # "api/v1/"
    list_namespace_url = ol_url + "/" + marquez_prefix_path + "namespaces"
    import requests
    try:
        response = requests.get(list_namespace_url)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Failed to connect to Marquez at {ol_url}/{ol_endpoint}/namespaces", e)
    log.info("Airflow is able to access the URL")


def _verify_atlan_http_backend(config):
    raise NotImplementedError("This feature is not implemented yet")


def _verify_custom_backend(config):
    raise NotImplementedError("This feature is not implemented yet")


with DAG(
    dag_id='OpenLineage_setup_check2',
    start_date=days_ago(1),
    description='A DAG to check OpenLineage setup and configurations',
    schedule_interval=None,
) as dag:

    is_ol_installed_task = PythonOperator(
        task_id='is_ol_installed',
        python_callable=is_ol_installed,
    )

    is_ol_accessible_and_enabled_task = PythonOperator(
        task_id='is_ol_accessible_and_enabled',
        python_callable=is_ol_accessible_and_enabled,
    )

    validate_connection_task = PythonOperator(
        task_id='validate_connection',
        python_callable=validate_connection,
    )

    is_ol_installed_task >> is_ol_accessible_and_enabled_task
    is_ol_accessible_and_enabled_task >> validate_connection_task
