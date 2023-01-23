# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import inspect
import logging
import os
import sys
from typing import Optional, Type, Union

from openlineage.client.transport.noop import NoopConfig, NoopTransport
from openlineage.client.transport.transport import Config, Transport, TransportFactory
from openlineage.client.utils import try_import_from_string

log = logging.getLogger(__name__)


try:
    import yaml
except ImportError:
    log.warning("ImportError occurred when trying to import yaml module.")


class DefaultTransportFactory(TransportFactory):
    def __init__(self):
        self.transports = {}

    def register_transport(self, type: str, clazz: Union[Type[Transport], str]):
        self.transports[type] = clazz

    def create(self) -> Transport:
        if os.environ.get("OPENLINEAGE_DISABLED", False) in [True, "true", "True"]:
            return NoopTransport(NoopConfig())

        if 'yaml' in sys.modules:
            yml_config = self._try_config_from_yaml()
            if yml_config:
                return self._create_transport(yml_config)
        # Fallback to setting HTTP transport from env variables
        http = self._try_http_from_env_config()
        if http:
            return http
        # If there is no HTTP transport, log events to console
        from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport
        log.warning("Couldn't initialize transport; will print events to console.")
        return ConsoleTransport(ConsoleConfig())

    def _create_transport(self, config: dict):
        transport_type = config['type']

        if transport_type in self.transports:
            transport_class = self.transports[transport_type]
        else:
            transport_class = transport_type

        if isinstance(transport_class, str):
            transport_class = try_import_from_string(transport_class)
        if not inspect.isclass(transport_class) or not issubclass(transport_class, Transport):
            raise TypeError(
                f"Transport {transport_class} has to be class, and subclass of Transport"
            )

        config_class = transport_class.config

        if isinstance(config_class, str):
            config_class = try_import_from_string(config_class)
        if not inspect.isclass(config_class) or not issubclass(config_class, Config):
            raise TypeError(f"Config {config_class} has to be class, and subclass of Config")

        return transport_class(config_class.from_dict(config))

    def _try_config_from_yaml(self) -> Optional[dict]:
        file = self._find_yaml()
        if file:
            try:
                with open(file, 'r') as f:
                    config = yaml.safe_load(f)
                    return config['transport']
            except Exception:
                # Just move to read env vars
                pass
        return None

    @staticmethod
    def _find_yaml() -> Optional[str]:
        # Check OPENLINEAGE_CONFIG env variable
        path = os.getenv('OPENLINEAGE_CONFIG', None)
        try:
            if path and os.path.isfile(path) and os.access(path, os.R_OK):
                return path
        except Exception:
            if path:
                log.exception(f"Couldn't read file {path}: ")
            else:
                # We can get different errors depending on system
                pass

        # Check current working directory:
        try:
            cwd = os.getcwd()
            if 'openlineage.yml' in os.listdir(cwd):
                return os.path.join(cwd, 'openlineage.yml')
        except Exception:
            # We can get different errors depending on system
            pass

        # Check $HOME/.openlineage dir
        try:
            path = os.path.expanduser("~/.openlineage")
            if 'openlineage.yml' in os.listdir(path):
                return os.path.join(path, 'openlineage.yml')
        except Exception:
            # We can get different errors depending on system
            pass
        return None

    @staticmethod
    def _try_http_from_env_config() -> Optional[Transport]:
        from openlineage.client.transport.http import (
            HttpConfig,
            HttpTransport,
            create_token_provider,
        )
        # backwards compatibility: create Transport from
        # OPENLINEAGE_URL and OPENLINEAGE_API_KEY
        if 'OPENLINEAGE_URL' not in os.environ:
            log.error("Did not find openlineage.yml and OPENLINEAGE_URL is not set")
            return None
        config = HttpConfig(
            url=os.environ['OPENLINEAGE_URL'],
            auth=create_token_provider({
                "type": "api_key",
                "api_key": os.environ.get('OPENLINEAGE_API_KEY', None)
            })
        )
        return HttpTransport(config)
