# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
from openlineage.client.facet import set_producer as set_producer_v1
from openlineage.client.facet_v2 import set_producer as set_producer_v2


def set_producer(producer: str) -> None:
    set_producer_v1(producer)
    set_producer_v2(producer)


__all__ = ["OpenLineageClient", "OpenLineageClientOptions", "set_producer"]
