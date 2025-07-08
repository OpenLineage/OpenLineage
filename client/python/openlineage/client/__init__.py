# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet_v2 import set_producer as set_producer_v2


def set_producer(producer: str) -> None:
    set_producer_v2(producer)


__all__ = ["OpenLineageClient", "OpenLineageClientOptions", "set_producer"]
