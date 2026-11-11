# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re

_SUFFIX = ".datawarehouse.fabric.microsoft.com"
# Fabric sometimes adds a short label such as ".z8b" right before the suffix
_ZONE_LABEL = re.compile(r"\.z[0-9a-f]{2}" + re.escape(_SUFFIX) + "$")
# "host", "host:port" or "host,port"
_HOST_AND_PORT = re.compile(r"^([^:,]+)(?:[:,](\d+))?$")
_DEFAULT_PORT = "1433"


def canonical_fabric_endpoint(server: str, port: int | str | None = None) -> str:
    """Normalize a Microsoft Fabric warehouse address.

    The same Fabric warehouse address can be written in several ways. Unless they are
    normalized, the same table shows up as different datasets depending on who reports it.
    This function:

    - lowercases the address;
    - removes the ".z<two characters>" label that Fabric sometimes adds right before
      ".datawarehouse.fabric.microsoft.com";
    - removes the port when it is 1433, the default, and keeps any other port.

    The OpenLineage Spark integration uses the same rules, so dbt and Spark report the
    same table under the same name.
    """
    value = server.strip().lower()
    match = _HOST_AND_PORT.match(value)
    if not match:
        return value
    host = _ZONE_LABEL.sub(_SUFFIX, match.group(1))
    endpoint_port = str(port).strip() if port is not None else match.group(2)
    if not endpoint_port or endpoint_port == _DEFAULT_PORT:
        return host
    return f"{host}:{endpoint_port}"
