# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.client.run import RunEvent

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


class Config:
    @classmethod
    def from_dict(cls, params: dict):
        return cls()


class Transport:
    kind = None
    config = Config

    def emit(self, event: RunEvent):
        raise NotImplementedError()


class TransportFactory:
    def create(self) -> Transport:  # type: ignore
        pass
