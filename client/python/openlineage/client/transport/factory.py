# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import inspect
import logging
import os

from openlineage.client.transport.noop import NoopConfig, NoopTransport
from openlineage.client.transport.transport import Config, Transport, TransportFactory
from openlineage.client.utils import try_import_from_string

log = logging.getLogger(__name__)


class DefaultTransportFactory(TransportFactory):
    def __init__(self) -> None:
        self.transports: dict[str, type[Transport] | str] = {}

    def register_transport(self, of_type: str, clazz: type[Transport] | str) -> None:
        self.transports[of_type] = clazz

    def create(self, config: dict[str, str] | None = None) -> Transport:
        if os.getenv("OPENLINEAGE_DISABLED", "").lower() == "true":
            return NoopTransport(NoopConfig())

        if config:
            return self._create_transport(config)

        # Fallback to setting HTTP transport from env variables
        http = self._try_http_from_env_config()
        if http:
            return http
        # If there is no HTTP transport, log events to console
        from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

        log.warning("Couldn't initialize transport; will print events to console.")
        return ConsoleTransport(ConsoleConfig())

    def _create_transport(self, config: dict[str, str]) -> Transport:
        try:
            transport_type = config["type"]
        except IndexError:
            msg = "You need to pass transport type in config."
            raise TypeError(msg) from None

        transport_class_type_or_str = self.transports.get(transport_type, transport_type)

        if isinstance(transport_class_type_or_str, str):
            transport_class = try_import_from_string(transport_class_type_or_str)
        else:
            transport_class = transport_class_type_or_str
        if not inspect.isclass(transport_class) or not issubclass(transport_class, Transport):
            msg = f"Transport {transport_class} has to be class, and subclass of Transport"
            raise TypeError(msg)

        config_class = transport_class.config_class

        if isinstance(config_class, str):
            config_class = try_import_from_string(config_class)
        if not inspect.isclass(config_class) or not issubclass(config_class, Config):
            msg = f"Config {config_class} has to be class, and subclass of Config"
            raise TypeError(msg)

        return transport_class(config_class.from_dict(config))  # type: ignore[call-arg]

    @staticmethod
    def _try_http_from_env_config() -> Transport | None:
        from openlineage.client.transport.http import (
            HttpConfig,
            HttpTransport,
            create_token_provider,
        )

        # backwards compatibility: create Transport from
        # OPENLINEAGE_URL, OPENLINEAGE_ENDPOINT, and OPENLINEAGE_API_KEY
        if "OPENLINEAGE_URL" not in os.environ:
            log.error("Did not find openlineage.yml and OPENLINEAGE_URL is not set")
            return None
        config = HttpConfig(
            url=os.environ["OPENLINEAGE_URL"],
            auth=create_token_provider(
                {
                    "type": "api_key",
                    "apiKey": os.environ.get("OPENLINEAGE_API_KEY", ""),
                },
            ),
        )
        endpoint = os.environ.get("OPENLINEAGE_ENDPOINT", None)
        if endpoint is not None:
            config.endpoint = endpoint

        return HttpTransport(config)
