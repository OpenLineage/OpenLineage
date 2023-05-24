# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

import attr

from openlineage.client.filter import Filter, create_filter
from openlineage.client.serde import Serde
from openlineage.client.utils import load_config

if TYPE_CHECKING:
    from requests import Session
    from requests.adapters import HTTPAdapter

from openlineage.client.run import RunEvent
from openlineage.client.transport import Transport, TransportFactory, get_default_factory
from openlineage.client.transport.http import HttpConfig, HttpTransport


@attr.s
class OpenLineageClientOptions:
    timeout: float = attr.ib(default=5.0)
    verify: bool = attr.ib(default=True)
    api_key: str = attr.ib(default=None)
    adapter: HTTPAdapter = attr.ib(default=None)


log = logging.getLogger(__name__)
_T = TypeVar("_T", bound="OpenLineageClient")


class OpenLineageClient:
    def __init__(  # noqa: PLR0913
        self,
        url: str | None = None,
        options: OpenLineageClientOptions | None = None,
        session: Session | None = None,
        transport: Transport | None = None,
        factory: TransportFactory | None = None,
    ) -> None:
        if factory is None:
            factory = get_default_factory()

        # Make config ellipsis - as a guard value to not try to
        # reload yaml each time config is referred to.
        self._config: dict[str, dict[str, str]] | None = None
        if url:
            # Backwards compatibility: if URL or options is set, use old path to initialize
            # HTTP transport.
            if not options:
                options = OpenLineageClientOptions()
            if not session:
                from requests import Session

                session = Session()
            self._initialize_url(url, options, session)
        elif transport:
            self.transport = transport
        else:
            transport_config = None if "transport" not in self.config else self.config["transport"]
            self.transport = factory.create(transport_config)

        self._filters: list[Filter] = []
        if "filters" in self.config:
            for conf in self.config["filters"]:
                _filter = create_filter(conf)
                if _filter:
                    self._filters.append(_filter)

    def _initialize_url(
        self,
        url: str,
        options: OpenLineageClientOptions,
        session: Session,
    ) -> None:
        self.transport = HttpTransport(
            HttpConfig.from_options(
                url=url,
                options=options,
                session=session,
            ),
        )

    def emit(self, event: RunEvent) -> None:
        if not isinstance(event, RunEvent):
            msg = "`emit` only accepts RunEvent class"
            raise ValueError(msg)  # noqa: TRY004
        if not self.transport:
            log.error("Tried to emit OpenLineage event, but transport is not configured.")
            return
        if log.isEnabledFor(logging.DEBUG):
            val = Serde.to_json(event).encode("utf-8")
            log.debug("OpenLineageClient will emit event %s", val)
        if self._filters and self.filter_event(event) is None:
            return
        if event:
            self.transport.emit(event)

    @classmethod
    def from_environment(cls: type[_T]) -> _T:
        return cls(transport=get_default_factory().create())

    @classmethod
    def from_dict(cls: type[_T], config: dict[str, str]) -> _T:
        return cls(transport=get_default_factory().create(config=config))

    def filter_event(self, event: RunEvent) -> RunEvent | None:
        """Filters jobs according to config-defined events"""
        for _filter in self._filters:
            if _filter.filter_event(event) is None:
                return None
        return event

    @property
    def config(self) -> dict[str, Any]:
        if self._config is None:
            self._config = load_config()
        return self._config
