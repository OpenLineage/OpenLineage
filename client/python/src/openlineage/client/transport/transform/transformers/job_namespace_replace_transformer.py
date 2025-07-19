# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from openlineage.client import event_v2
from openlineage.client.transport.transform.transform import EventTransformer

if TYPE_CHECKING:
    from openlineage.client.client import Event
    from openlineage.client.facet_v2 import parent_run

log = logging.getLogger(__name__)


class JobNamespaceReplaceTransformer(EventTransformer):
    """EventTransformer that replaces the Job namespace in OpenLineage events."""

    def __init__(self, properties: dict[str, Any]) -> None:
        if not properties.get("new_job_namespace"):
            msg = f"`new_job_namespace` not passed to {self.__class__.__name__}"
            raise RuntimeError(msg)
        self.new_job_namespace = properties["new_job_namespace"]
        self.include_parent_facet = str(properties.get("include_parent_facet", "true")).lower() == "true"
        super().__init__(properties=properties)

    def transform(self, event: Event) -> Event | None:
        if not isinstance(event, (event_v2.RunEvent, event_v2.JobEvent)):  # These events have Job
            log.warning(
                "Unsupported type: expected `RunEvent` or `JobEvent`, got `%s`. Skipping transformation.",
                type(event),
            )
            return event

        log.debug(
            "Replacing OpenLineage job namespace: `%s` with: `%s` for single event.",
            event.job.namespace,
            self.new_job_namespace,
        )
        event.job.namespace = self.new_job_namespace

        if not self.include_parent_facet:
            log.debug("Set `include_parent_facet` to True to include replacement in ParentRunFacet.")
            return event

        if not isinstance(event, event_v2.RunEvent):
            log.debug("Only `RunEvent` can have ParentRunFacet, skipping replacement in ParentRunFacet.")
            return event

        if not event.run.facets or not event.run.facets.get("parent"):
            log.debug("Parent run facet not found, skipping replacement in ParentRunFacet.")
            return event

        # Mypy sees RunFacet, but we assume ParentRunFacet, so we ignore the check.
        parent_facet: parent_run.ParentRunFacet = event.run.facets["parent"]  # type: ignore[assignment]
        log.debug(
            "Replacing parent OpenLineage job namespace: `%s` with: `%s` for single event.",
            parent_facet.job.namespace,
            self.new_job_namespace,
        )
        parent_facet.job.namespace = self.new_job_namespace

        if parent_facet.root:
            log.debug(
                "Replacing root OpenLineage job namespace: `%s` with: `%s` for single event.",
                parent_facet.root.job.namespace,
                self.new_job_namespace,
            )
            parent_facet.root.job.namespace = self.new_job_namespace

        return event
