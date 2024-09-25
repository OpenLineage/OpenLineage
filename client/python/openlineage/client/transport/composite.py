# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from functools import cached_property
from typing import TYPE_CHECKING, Any

import attr
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


@attr.s
class CompositeConfig(Config):
    """
    CompositeConfig is a configuration class for CompositeTransport.

    Attributes:
        transports:
            A list of dictionaries, where each dictionary represents the configuration
            for a child transport. Each dictionary should contain the necessary parameters
            to initialize a specific transport instance.

        continue_on_failure:
            If set to True, the CompositeTransport will attempt to emit the event using
            all configured transports, regardless of whether any previous transport
            in the list failed to emit the event. If set to False, an error in one
            transport will halt the emission process for subsequent transports.
    """

    transports: list[dict[str, Any]] | dict[str, dict[str, Any]] = attr.ib()
    continue_on_failure: bool = attr.ib(default=True)

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> CompositeConfig:
        """Create a CompositeConfig object from a dictionary."""
        if "transports" not in params:
            msg = "composite `transports` not passed to CompositeConfig"
            raise RuntimeError(msg)
        return cls(**get_only_specified_fields(cls, params))


class CompositeTransport(Transport):
    """CompositeTransport is a transport class that emits events using multiple transports."""

    kind = "composite"
    config_class = CompositeConfig

    def __init__(self, config: CompositeConfig) -> None:
        """Initialize a CompositeTransport object."""
        self.config = config

    @cached_property
    def transports(self) -> list[Transport]:
        """Create and return a list of transports based on the config."""
        from openlineage.client.transport import get_default_factory

        transports = []
        config_transports = self.config.transports
        if isinstance(config_transports, dict):
            config_transports = [
                {**config, "name": name} for name, config in config_transports.items() if config
            ]
        for transport_config in config_transports:
            transports.append(get_default_factory().create(transport_config))
        return transports

    def emit(self, event: Event) -> None:
        """Emit an event using all transports in the config."""
        for transport in self.transports:
            try:
                log.debug("Emitting event using %s", transport.kind)
                transport.emit(event)
            except Exception as e:  # noqa: BLE001
                if self.config.continue_on_failure:
                    log.warning("Transport %s failed to emit event with error: %s", transport.kind, e)
                else:
                    msg = f"Transport {transport.kind} failed to emit event"
                    raise RuntimeError(msg) from e
