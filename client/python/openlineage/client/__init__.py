# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.facet import set_producer

__all__ = [
    "OpenLineageClient",
    "OpenLineageClientOptions",
    "set_producer",
]
