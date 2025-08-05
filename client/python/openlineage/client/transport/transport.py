# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Transport interface for OpenLineage events.

To implement a custom Transport, implement both Config and Transport classes.

Transport implementation requirements:
 * Specify class variable `config_class` that points to the Config class that Transport requires
 * Implement `__init__` that accepts the specified Config class instance
 * Implement `emit` method that accepts OpenLineage events

Config implementation requirements:
 * Implement `from_dict` classmethod to create config from dictionary parameters
 * The config class can have complex attributes, but must be able to instantiate them in `from_dict`

Transport instantiation:
 * TransportFactory instantiates custom transports by looking at the `type` field in the config
 * The factory uses this type to determine which transport class to instantiate
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

import attr

if TYPE_CHECKING:
    from openlineage.client.client import Event


_T = TypeVar("_T", bound="Config")


@attr.define
class Config:
    @classmethod
    def from_dict(cls: type[_T], params: dict[str, Any]) -> _T:  # noqa: ARG003
        return cls()


class Transport:
    kind: str | None = None
    name: str | None = None
    priority: int = 0
    config_class: type[Config] = Config

    def emit(self, event: Event) -> Any:
        raise NotImplementedError

    def close(self, timeout: float = -1) -> bool:
        """
        Closes the transport, waiting for all events to complete until the timeout is reached.

        Params:
            timeout: Timeout in seconds. Negative value will block until last event
            is processed, while 0 means it completes immediately.

        Returns:
            bool: True if all events were processed before transport was closed,
                False if some events were not processed.
        """
        return True

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}(name={self.name}, kind={self.kind}, priority={self.priority})>"


class TransportFactory:
    def create(self, config: dict[str, str] | None = None) -> Transport:
        raise NotImplementedError
