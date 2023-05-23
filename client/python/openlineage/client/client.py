# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

import attr

from openlineage.client.serde import Serde

if TYPE_CHECKING:
    from requests import Session
    from requests.adapters import HTTPAdapter

from openlineage.client.run import RunEvent
from openlineage.client.transport import Transport, get_default_factory
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
    def __init__(
        self,
        url: str | None = None,
        options: OpenLineageClientOptions | None = None,
        session: Session | None = None,
        transport: Transport | None = None,
    ) -> None:
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
            self.transport = get_default_factory().create()

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
        else:
            if log.isEnabledFor(logging.DEBUG):
                val = Serde.to_json(event).encode("utf-8")
                log.debug("OpenLineageClient will emit event %s", val)
            self.transport.emit(event)

    @classmethod
    def from_environment(cls: type[_T]) -> _T:
        return cls(transport=get_default_factory().create())

    @classmethod
    def from_dict(cls: type[_T], config: dict[str, str]) -> _T:
        return cls(transport=get_default_factory().create(config=config))
