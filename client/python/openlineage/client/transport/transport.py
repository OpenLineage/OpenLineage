# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""
To implement custom Transport implement Config and Transport classes.

Transport needs to
 * specify class variable `config` that will point to Config class that Transport requires
 * __init__ that will accept specified Config class instance
 * implement `emit` method that will accept RunEvent

Config file is read and parameters there are passed to `from_dict` classmethod.
The config class can have more complex attributes, but needs to be able to
instantiate them in `from_dict` method.

DefaultTransportFactory instantiates custom transports by looking at `type` field in
class config.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from openlineage.client.client import Event


_T = TypeVar("_T", bound="Config")


class Config:
    @classmethod
    def from_dict(cls: type[_T], params: dict[str, Any]) -> _T:  # noqa: ARG003
        return cls()


class Transport:
    kind: str | None = None
    name: str | None = None
    config_class: type[Config] = Config

    def emit(self, event: Event) -> Any:
        raise NotImplementedError


class TransportFactory:
    def create(self, config: dict[str, str] | None = None) -> Transport:
        raise NotImplementedError
