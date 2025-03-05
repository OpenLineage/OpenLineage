---
sidebar_position: 4
title: Preflight Check Class
---
# Preflight Check Class

## Purpose

In some cases, you might want to validate your OpenLineage setup in Airflow without having to start Airflow services or trigger a pipeline. Or you might be looking for a way to validate OpenLineage within a task rather than use a DAG. In these cases, you can use this Python class instead of the [Preflight Check DAG](https://openlineage.io/docs/integrations/airflow/preflight-check-dag), which is the basis of this class. 

## Preflight Check Class Code

```python
from __future__ import annotations

import logging
import os
import attr

from packaging.version import Version

from airflow import DAG
from airflow.configuration import conf
from airflow import __version__ as airflow_version
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Set this to True to bypass the latest version check for OpenLineage package.
# Version check will be skipped if unable to access PyPI URL
BYPASS_LATEST_VERSION_CHECK = False
# Update this to `CUSTOM` if using any other backend for OpenLineage events ingestion
# When using custom transport - implement custom checks in _verify_custom_backend function
LINEAGE_BACKEND = "MARQUEZ"

log = logging.getLogger(__name__)

class CheckOpenLineage:
    """
    The preflight CheckOpenLineage class has been created to enable verifying of the setup 
    of OpenLineage within an Apache Airflow environment. It checks the Airflow version, the 
    version of the installed OpenLineage package, and the configuration settings read by the 
    OpenLineage listener. This validation is crucial because, after setting up OpenLineage 
    with Airflow and configuring necessary environment variables, users need confirmation 
    that the setup is correctly done to start receiving OpenLineage events.
    """


    def _get_latest_package_version(self, library_name: str) -> Version | None:
        """
        Get the latest available version of the Apache Airflow OpenLineage Provider package
        from the PyPI.org API.
        """
        try:
            import requests

            response = requests.get(f"https://pypi.org/pypi/{library_name}/json")
            response.raise_for_status()
            version_string = response.json()["info"]["version"]
            return Version(version_string)
        except Exception as e:
            log.error(f"Failed to fetch latest version for `{library_name}` from PyPI: {e}")
            return None


    def _get_installed_package_version(self, library_name) -> Version | None:
        """
        Get the version of Apache Airflow OpenLineage Provider installed locally.
        """
        try:
            from importlib.metadata import version

            version = Version(version(library_name)) 
            log.info(f"Installed {library_name} version is {version}.")
            return version
        except Exception as e:
            raise ModuleNotFoundError(f"`{library_name}` is not installed") from e


    def _provider_can_be_used(self) -> [bool, str]:
        """
        Get the version of the locally installed Apache Airflow instance to determine if the
        Apache Airflow OpenLineage Provider can be used.
        """
        import subprocess
        
        app_name = "airflow"
        version_flag = "version"
        process = subprocess.run([app_name, version_flag], capture_output=True, text=True, check=True)
        version_output = process.stdout.strip()
        parsed_version = Version(version_output)
        if parsed_version < Version("2.1"):
            raise RuntimeError("OpenLineage is not supported in Airflow versions <2.1")
        elif parsed_version >= Version("2.8"):
            log.info("OpenLineage Provider can be used.")
            return True, version_output
        return False, version_output


    def validate_ol_installation(self) -> None:
        """
        Validate the OpenLineage installation by verifying the compatibility of the OpenLineage integration and the
        locally installed copy of Apache Airflow.
        """
        library_name = "openlineage-airflow"
        provider_status = self._provider_can_be_used()
        if provider_status[0]:
            library_name = "apache-airflow-providers-openlineage"
            library_version = self._get_installed_package_version(library_name)

            if Version(provider_status[1]) >= Version("2.9.0") and library_version < Version("2.0.0"):
                raise ValueError(
                    f"Airflow version `{provider_status[1]}` requires `{library_name}` version >=2.0.0. "
                    f"Installed version: `{library_version}` "
                    f"Please upgrade the package using `pip install --upgrade {library_name}`"
                )
            elif Version(provider_status[1]) >= Version("2.8.0") and library_version < Version("1.11.0"):
                raise ValueError(
                    f"Airflow version `{provider_status[1]}` requires `{library_name}` version >=1.11.0. "
                    f"Installed version: `{library_version}` "
                    f"Please upgrade the package using `pip install --upgrade {library_name}`"
                )

            if BYPASS_LATEST_VERSION_CHECK:
	            log.info(f"Bypassing the latest version check for `{library_name}`")
	            return

	        latest_version = self._get_latest_package_version(library_name)
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

        else:
            library_version = self._get_installed_package_version(library_name)
            if Version(provider_status[1]) < Version("1.11.0"):
                raise ValueError(
                    f"Airflow version `{provider_status[1]}` is no longer supported as of October 2022. "
                    f"Consider upgrading to a more recent version of Airflow. " 
                    f"If upgrading to Airflow >=2.7.0, use the OpenLineage Airflow Provider. "
                )

    def _is_transport_set(self) -> None:
        """Check if an OpenLineage transport has been set."""
        transport = conf.get("openlineage", "transport", fallback="")
        log.info(f"Transport: {transport}")
        if transport:
            raise ValueError(
                "Transport value found: `%s`\n"
                "Please check the format at "
                "https://openlineage.io/docs/client/python/#built-in-transport-types",
                transport,
            )
        log.info("Airflow OpenLineage transport is not set.")
        return


    def _is_config_set(self, provider: bool = True) -> None:
        """Check if an OpenLineage config exists."""
        if provider:
            config_path = conf.get("openlineage", "config_path", fallback="")
        else:
            config_path = os.getenv("OPENLINEAGE_CONFIG", "")

        log.info("OpenLineage config is not set.")
        return


    def _check_http_env_vars(self) -> None:
        """Check environment for OpenLineage URL and endpoint environment variables."""
        from urllib.parse import urljoin

        try:
            final_url = urljoin(os.getenv("OPENLINEAGE_URL", ""), os.getenv("OPENLINEAGE_ENDPOINT", ""))
            log.info("OPENLINEAGE_URL and OPENLINEAGE_ENDPOINT are set to: %s", final_url)
        except:
            raise ValueError(
                "OPENLINEAGE_URL and OPENLINEAGE_ENDPOINT are not set. "
                "Please set up OpenLineage using documentation at "
                "https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html"
            )

        transport_var = os.getenv("AIRFLOW__OPENLINEAGE__TRANSPORT", "")
        if transport_var:
            log.info("AIRFLOW__OPENLINEAGE__TRANSPORT is set to: %s", transport_var)
        else:
            log.info("AIRFLOW__OPENLINEAGE__TRANSPORT variable is not set.")

        return


    def _debug_missing_transport(self):
        """Debug a missing transport."""
        if self._provider_can_be_used():
            self._is_config_set(provider=True)
            self._is_transport_set()
        self._is_config_set(provider=False)
        # self._check_openlineage_yml("openlineage.yml")
        # self._check_openlineage_yml("~/.openlineage/openlineage.yml")
        self._check_http_env_vars()
        raise ValueError("OpenLineage is missing configuration, please refer to the OL setup docs.")


    def _is_listener_accessible(self):
        """Check if an OpenLineage listener is accessible."""
        if self._provider_can_be_used():
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


    def _is_ol_disabled(self):
        """Confirm that OpenLineage is not disabled and inspect the configuration to suggest a fix."""
        if self._provider_can_be_used():
            try:
                # apache-airflow-providers-openlineage >= 1.7.0
                from airflow.providers.openlineage.conf import is_disabled
            except ImportError:
                # apache-airflow-providers-openlineage < 1.7.0
                from airflow.providers.openlineage.plugins.openlineage import _is_disabled as is_disabled
        else:
            from openlineage.airflow.plugin import _is_disabled as is_disabled

        if is_disabled():
            if self._provider_can_be_used() and conf.getboolean("openlineage", "disabled", fallback=False):
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
        log.info("OpenLineage is not disabled.")
        return False


    def _get_transport(self):
        """Get the configured transport from the OpenLineage plugin."""
        if self._provider_can_be_used():
            from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin
            transport = OpenLineageProviderPlugin().listeners[0].adapter.get_or_create_openlineage_client().transport
        else:
            from openlineage.airflow.plugin import OpenLineagePlugin
            transport = (
                OpenLineagePlugin.listeners[0].adapter.get_or_create_openlineage_client().transport
            )
        return transport

    def is_ol_accessible_and_enabled(self):
        """Confirm that OpenLineage is accessible and enabled by attempting to build the transport."""
        if not self._is_listener_accessible():
            self._is_ol_disabled()

        try:
            transport = self._get_transport()
        except Exception as e:
            raise ValueError("There was an error when trying to build transport.") from e

        if transport is None or transport.kind in ("noop", "console"):
            _debug_missing_transport()


    def validate_connection(self):
        """Validate the connection to the lineage backend."""
        transport = self._get_transport()
        config = attr.asdict(transport.config)
        self._verify_backend(LINEAGE_BACKEND, config)


    def _verify_backend(self, backend_type: str, config: dict):
        """Verify the lineage backed."""
        backend_type = backend_type.lower()
        if backend_type == "marquez":
            log.info("Backend type: Marquez")
            return
        elif backend_type == "atlan":
            log.info("Backend type: Atlan")
            return self._verify_atlan_http_backend(config)
        elif backend_type == "custom":
            log.info("Backend type: custom")
            return self._verify_custom_backend(config)
        raise ValueError(f"Unsupported backend type: {backend_type}")


    def _verify_atlan_http_backend(self, config):
        raise NotImplementedError("This feature is not implemented yet")


    def _verify_custom_backend(self, config):
        raise NotImplementedError("This feature is not implemented yet")

```
