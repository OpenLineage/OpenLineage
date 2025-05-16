# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from openlineage.client import event_v2
from openlineage.client.transport.transform.transform import EventTransformer

if TYPE_CHECKING:
    from openlineage.client.client import Event

log = logging.getLogger(__name__)


class JobNamespaceReplaceTransformer(EventTransformer):
    """EventTransformer that replaces the Job namespace in OpenLineage events."""

    def __init__(self, properties: dict[str, Any]) -> None:
        if not properties.get("new_job_namespace"):
            msg = f"`new_job_namespace` not passed to {self.__class__.__name__}"
            raise RuntimeError(msg)
        super().__init__(properties=properties)

    def transform(self, event: Event) -> Event | None:
        if not isinstance(event, (event_v2.RunEvent, event_v2.JobEvent)):  # These events have Job
            log.warning(
                "Unsupported type: expected `RunEvent` or `JobEvent`, got `%s`. Skipping transformation.",
                type(event),
            )
            return event

        new_namespace = self.properties["new_job_namespace"]
        log.debug(
            "Replacing current OpenLineage job namespace: `%s` with new: `%s` for single event.",
            event.job.namespace,
            new_namespace,
        )

        event.job.namespace = new_namespace

        return event
