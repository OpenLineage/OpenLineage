# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from openlineage.client import event_v2
from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Config, Transport

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


@dataclass
class AmazonDataZoneConfig(Config):
    domain_id: str
    endpoint_override: str | None = None

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> AmazonDataZoneConfig:
        if "domainId" not in params:
            msg = "`domainId` key not passed to AmazonDataZoneConfig"
            raise RuntimeError(msg)
        return cls(domain_id=params["domainId"], endpoint_override=params.get("endpointOverride"))


class AmazonDataZoneTransport(Transport):
    kind = "amazon_datazone_api"
    config_class = AmazonDataZoneConfig

    def __init__(self, config: AmazonDataZoneConfig) -> None:
        self.config = config
        self.datazone = None

        self._setup_datazone(self.config.endpoint_override)
        log.debug(
            "Constructing OpenLineage transport that will send events to Amazon DataZone domain `%s`.",
            config.domain_id,
        )

    def emit(self, event: Event) -> None:
        if not isinstance(event, (RunEvent, event_v2.RunEvent)):
            # DataZone only supports RunEvent
            log.warning("DataZone only supports RunEvent")
            return
        try:
            response = self.datazone.post_lineage_event(  # type: ignore[attr-defined]
                domainIdentifier=self.config.domain_id, event=Serde.to_json(event).encode("utf-8")
            )
            log.info(
                "Successfully posted a LineageEvent: %s in Domain: %s", response["id"], response["domainId"]
            )
        except Exception as error:
            msg = f"Failed to send lineage event to DataZone Domain {self.config.domain_id}: {event}"
            raise RuntimeError(msg) from error

    def close(self, timeout: float = -1) -> bool:
        if timeout >= 0:
            log.warning("AmazonDataZoneTransport does not support timeout")
        self.datazone.close()  # type: ignore[attr-defined]
        return True

    def _setup_datazone(self, endpoint_url: str | None = None) -> None:
        try:
            import boto3  # type: ignore[import-not-found]

            self.datazone = boto3.client("datazone", endpoint_url=endpoint_url)
        except (ImportError, ModuleNotFoundError):
            log.exception(
                "OpenLineage client could not import the boto3 module. "
                "This could be due to boto3 not being installed or being corrupted. "
                "DataZoneTransport requires boto3 to function properly. "
                "Please ensure boto3 is installed correctly. "
                "You can install or reinstall it via `pip install boto3`"
            )
            raise
