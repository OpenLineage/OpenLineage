# Copyright 2018-2025 contributors to the OpenLineage project
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


@attr.define
class CompositeConfig(Config):
    """
    CompositeConfig is a configuration class for CompositeTransport.

    Attributes:
        transports:
            A list or dict of dictionaries, where each dictionary represents the configuration
            for a child transport. Each dictionary should contain the necessary parameters
            to initialize a specific transport instance. If dict of dictionaries is passed,
            keys of the dict will be treated as transport names.

        continue_on_failure:
            If set to True, the CompositeTransport will attempt to emit the event using
            all configured transports, regardless of whether any previous transport
            in the list failed to emit the event. If none of the transports successfully emit
            the event, an error will still be raised at the end to indicate that no events were emitted.
            If set to False, an error in transport will halt the emission process for subsequent transports.

        continue_on_success:
            If True, the CompositeTransport will continue emitting events to all transports
            even after a successful delivery. If False, it will stop emitting to other
            transports as soon as the first successful delivery occurs.

        sort_transports:
            If True, transports will be sorted before emission by `priority` field.
            If False, order of transports will not be changed.

    Continue behavior summary:
        - continue_on_failure=True, continue_on_success=True:
            Always emits to all transports, regardless of successes or failures. Events are delivered
            everywhere, with no early termination.

        - continue_on_failure=True, continue_on_success=False:
            Stops on the first successful emission. Failures are ignored and transports continue to be
            tried until a success is found.

        - continue_on_failure=False, continue_on_success=False:
            Stops on the first success or the first failure, whichever occurs first.

        - continue_on_failure=False, continue_on_success=True:
            Stops immediately on the first failure (fail-fast). However, if no failures occur, events will be
            emitted to all transports, effectively behaving like a "fail-fast all" strategy. This behavior
            may not align with typical expectations when an early stop after a success is desired; review
            carefully to ensure it matches the intended delivery semantics.
    """

    transports: list[dict[str, Any]] | dict[str, dict[str, Any]]
    continue_on_failure: bool = True
    continue_on_success: bool = True
    sort_transports: bool = False

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
        if not self.transports:
            msg = "CompositeTransport initialization failed: No transports found"
            raise ValueError(msg)
        log.debug(
            "Constructing OpenLineage composite transport with the following transports: %s",
            [str(x) for x in self.transports],  # to use str and not repr
        )

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
        if self.config.sort_transports:
            # Default priority is 0 - reverse sorting to prioritize higher values
            transports = sorted(transports, key=lambda t: t.priority, reverse=True)
        return transports

    def emit(self, event: Event) -> None:
        """Emit an event using all transports in the config."""
        _success_count, _failure_count = 0, 0
        for transport in self.transports:
            try:
                log.debug("Emitting event using transport %s", transport)
                transport.emit(event)
                _success_count += 1
            except Exception as e:  # Handle failure
                _failure_count += 1
                if self.config.continue_on_failure:
                    log.warning("Transport %s failed to emit event with error: %s", transport, e)
                    log.debug("OpenLineage emission failure details:", exc_info=True)
                else:
                    msg = f"Transport {transport} failed to emit event"
                    raise RuntimeError(msg) from e
            else:  # Handle success
                log.debug("Event successfully emitted with transport %s", transport)
                if not self.config.continue_on_success:
                    log.info(
                        "Stopping OpenLineage CompositeTransport emission after the first "
                        "successful delivery because `continue_on_success=False`. "
                        "Transport that emitted the event: %s",
                        transport,
                    )
                    return

        if _success_count == 0:
            msg = (
                f"None of the transports successfully emitted the event: "
                f"{[str(x) for x in self.transports]}"  # to use str and not repr
            )
            raise RuntimeError(msg)

        log.debug(
            "CompositeTransport: finished emitting OpenLineage events;"
            " %s transports failed,  %s transports succeeded",
            _failure_count,
            _success_count,
        )
        return

    def close(self, timeout: float = -1) -> bool:
        result = True
        last_exception: Exception | None = None
        for transport in self.transports:
            try:
                result = transport.close(timeout) and result
            except Exception as e:
                log.exception("Error while closing transport %s", transport)
                last_exception = e

        if last_exception:
            raise last_exception
        return result
