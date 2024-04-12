from __future__ import annotations

import logging
import os

from packaging.version import Version

from airflow import DAG
from airflow.configuration import conf
from airflow import __version__ as airflow_version
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Set this to True to bypass the latest version check for OpenLineage package.
# Version check will be skipped if unable to access PyPI URL
BYPASS_LATEST_VERSION_CHECK = False
# Update this if using any other backend for OpenLineage events ingestion
# When using custom transport - implement custom checks in _verify_custom_backend function
LINEAGE_BACKEND = "MARQUEZ"

log = logging.getLogger(__name__)


def _get_latest_package_version(library_name: str) -> Version | None:
    try:
        import requests

        response = requests.get(f"https://pypi.org/pypi/{library_name}/json")
        response.raise_for_status()
        version_string = response.json()["info"]["version"]
        return Version(version_string)
    except Exception as e:
        log.error(f"Failed to fetch latest version for `{library_name}` from PyPI: {e}")
        return None


def _get_installed_package_version(library_name) -> Version | None:
    try:
        from importlib.metadata import version

        return Version(version(library_name))
    except Exception as e:
        raise ModuleNotFoundError(f"`{library_name}` is not installed") from e


def _provider_can_be_used() -> bool:
    parsed_version = Version(airflow_version)
    if parsed_version < Version("2.1"):
        raise RuntimeError("OpenLineage is not supported in Airflow versions <2.1")
    elif parsed_version >= Version("2.7"):
        return True
    return False


def validate_ol_installation() -> None:
    library_name = "openlineage-airflow"
    if _provider_can_be_used():
        library_name = "apache-airflow-providers-openlineage"

    library_version = _get_installed_package_version(library_name)
    if BYPASS_LATEST_VERSION_CHECK:
        log.info(f"Bypassing the latest version check for `{library_name}`")
        return

    latest_version = _get_latest_package_version(library_name)
    if latest_version is None:
        log.warning(f"Failed to fetch the latest version for `{library_name}`. Skipping version check.")
        return

    if library_version < latest_version:
        raise ValueError(
            f"`{library_name}` is out of date. "
            f"Installed version: `{library_version}`, "
            f"Required version: `{latest_version}`"
            f"Please upgrade the package using `pip install --upgrade {library_name}` or set BYPASS_LATEST_VERSION_CHECK to True"
        )


def _is_transport_set() -> None:
    transport = conf.get("openlineage", "transport", fallback="")
    if transport:
        raise ValueError(
            "Transport value found: `%s`\n"
            "Please check the format at "
            "https://openlineage.io/docs/client/python/#built-in-transport-types",
            transport,
        )
    log.info("Airflow OL transport is not set.")
    return


def _is_config_set(provider: bool = True) -> None:
    if provider:
        config_path = conf.get("openlineage", "config_path", fallback="")
    else:
        config_path = os.getenv("OPENLINEAGE_CONFIG", "")

    if config_path and not _check_openlineage_yml(config_path):
        raise ValueError(
            "Config file is empty or does not exist: `%s`",
            config_path,
        )

    log.info("OL config is not set.")
    return


def _check_openlineage_yml(file_path) -> bool:
    file_path = os.path.expanduser(file_path)
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            content = file.read()
        if not content:
            raise ValueError(f"Empty file: `{file_path}`")
        raise ValueError(
                f"File found at `{file_path}` with the following content: `{content}`. "
                "Make sure there the configuration is correct."
            )
    log.info("File not found: `%s`", file_path)
    return False


def _check_http_env_vars() -> None:
    from urllib.parse import urljoin

    final_url = urljoin(os.getenv("OPENLINEAGE_URL", ""), os.getenv("OPENLINEAGE_ENDPOINT", ""))
    if final_url:
        raise ValueError("OPENLINEAGE_URL and OPENLINEAGE_ENDPOINT are set to: %s", final_url)
    log.info(
        "OPENLINEAGE_URL and OPENLINEAGE_ENDPOINT are not set. "
        "Please set up OpenLineage using documentation at "
        "https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html"
    )
    return


def _debug_missing_transport():
    if _provider_can_be_used():
        _is_config_set(provider=True)
        _is_transport_set()
    _is_config_set(provider=False)
    _check_openlineage_yml("openlineage.yml")
    _check_openlineage_yml("~/.openlineage/openlineage.yml")
    _check_http_env_vars()
    raise ValueError("OpenLineage is missing configuration, please refer to the OL setup docs.")


def _is_listener_accessible():
    if _provider_can_be_used():
        try:
            from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin as plugin
        except ImportError as e:
            raise ValueError("OpenLineage provider is not accessible") from e
    else:
        try:
            from openlineage.airflow.plugin import OpenLineagePlugin as plugin
        except ImportError as e:
            raise ValueError("OpenLineage is not accessible") from e

    if len(plugin.listeners) == 1:
        return True

    return False


def _is_ol_disabled():
    if _provider_can_be_used():
        try:
            # apache-airflow-providers-openlineage >= 1.7.0
            from airflow.providers.openlineage.conf import is_disabled
        except ImportError:
            # apache-airflow-providers-openlineage < 1.7.0
            from airflow.providers.openlineage.plugins.openlineage import _is_disabled as is_disabled
    else:
        from openlineage.airflow.plugin import _is_disabled as is_disabled

    if is_disabled():
        if _provider_can_be_used() and conf.getboolean("openlineage", "disabled", fallback=False):
            raise ValueError("OpenLineage is disabled in airflow.cfg: openlineage.disabled")
        elif os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true":
            raise ValueError(
                "OpenLineage is disabled due to the environment variable OPENLINEAGE_DISABLED"
            )
        raise ValueError(
            "OpenLineage is disabled because required config/env variables are not set. "
            "Please refer to "
            "https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html"
        )
    return False


def _get_transport():
    if _provider_can_be_used():
        from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin
        transport = OpenLineageProviderPlugin().listeners[0].adapter.get_or_create_openlineage_client().transport
    else:
        from openlineage.airflow.plugin import OpenLineagePlugin
        transport = (
            OpenLineagePlugin.listeners[0].adapter.get_or_create_openlineage_client().transport
        )
    return transport

def is_ol_accessible_and_enabled():
    if not _is_listener_accessible():
        _is_ol_disabled()

    try:
        transport = _get_transport()
    except Exception as e:
        raise ValueError("There was an error when trying to build transport.") from e

    if transport is None or transport.kind in ("noop", "console"):
        _debug_missing_transport()


def validate_connection():
    transport = _get_transport()
    config = extract_config(transport)
    verify_backend(LINEAGE_BACKEND, config)


def extract_config(transport):
    if transport.kind == "http":
        return _extract_http_config(transport)
    elif transport.kind == "kafka":
        return _extract_kafka_config(transport)
    elif transport.kind == "file":
        return _extract_file_config(transport)
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
    if backend_type == "marquez":
        return _verify_marquez_http_backend(config)
    elif backend_type == "atlan":
        return _verify_atlan_http_backend(config)
    elif backend_type == "marquez":
        return _verify_custom_backend(config)
    raise ValueError(f"Unsupported backend type: {backend_type}")


def _verify_marquez_http_backend(config):
    log.info("Checking Marquez setup")
    ol_url = config["url"]
    ol_endpoint = config["endpoint"]  # "api/v1/lineage"
    marquez_prefix_path = ol_endpoint[: ol_endpoint.rfind("/") + 1]  # "api/v1/"
    list_namespace_url = ol_url + "/" + marquez_prefix_path + "namespaces"
    import requests

    try:
        response = requests.get(list_namespace_url)
        response.raise_for_status()
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Marquez at `{list_namespace_url}`") from e
    log.info("Airflow is able to access the URL")


def _verify_atlan_http_backend(config):
    raise NotImplementedError("This feature is not implemented yet")


def _verify_custom_backend(config):
    raise NotImplementedError("This feature is not implemented yet")


with DAG(
    dag_id="OpenLineage_setup_check",
    start_date=days_ago(1),
    description="A DAG to check OpenLineage setup and configurations",
    schedule_interval="@once",
) as dag:
    validate_ol_installation_task = PythonOperator(
        task_id="validate_ol_installation",
        python_callable=validate_ol_installation,
    )

    is_ol_accessible_and_enabled_task = PythonOperator(
        task_id="is_ol_accessible_and_enabled",
        python_callable=is_ol_accessible_and_enabled,
    )

    validate_connection_task = PythonOperator(
        task_id="validate_connection",
        python_callable=validate_connection,
    )

    validate_ol_installation_task >> is_ol_accessible_and_enabled_task
    is_ol_accessible_and_enabled_task >> validate_connection_task
