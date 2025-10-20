# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Common constants and utilities shared between HTTP and Async HTTP transports."""

from typing import Any

# Default retry configuration for HTTP transports
DEFAULT_RETRY_CONFIG: dict[str, Any] = {
    "total": 5,
    "read": 5,
    "connect": 5,
    "backoff_factor": 0.3,
    "status_forcelist": [500, 502, 503, 504],
    "allowed_methods": ["HEAD", "POST"],
}
