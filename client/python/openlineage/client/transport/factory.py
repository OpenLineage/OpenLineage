# Copyright 2018-2025 contributors to the OpenLineage project
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
        """
        Initializes and returns a transport mechanism based on the provided configuration.

        If 'OPENLINEAGE_DISABLED' is set to 'true', a NoopTransport instance is returned,
        effectively disabling transport.
        If a configuration dictionary is provided, transport specified by the config is initialized.
        If no configuration is provided, the function defaults to a console-based transport, logging
        a warning and printing events to the console.
        """
        if os.getenv("OPENLINEAGE_DISABLED", "").lower().strip() == "true":
            log.info("OpenLineage is disabled. No events will be emitted.")
            return NoopTransport(NoopConfig())

        if config:
            return self._create_transport(config)

        # If no config is passed, log events to console
        from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

        log.warning("Couldn't initialize OpenLineage transport; will print events to console.")
        return ConsoleTransport(ConsoleConfig())

    def _create_transport(self, config: dict[str, str]) -> Transport:
        try:
            transport_type = config["type"]
        except IndexError:
            msg = "You need to pass transport type in config."
            raise TypeError(msg) from None

        transport_name = config.pop("name", None)
        transport_priority = config.pop("priority", 0)
        try:
            transport_priority = int(transport_priority)
        except ValueError as e:
            msg = f"Error casting priority `{transport_priority}` to int for transport `{transport_type}`"
            raise ValueError(msg) from e

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

        transport: Transport = transport_class(config_class.from_dict(config))  # type: ignore[call-arg]
        if transport_name and not transport.name:
            transport.name = transport_name
        if transport_priority and transport.priority == 0:
            transport.priority = transport_priority
        return transport
