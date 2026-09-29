# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Common constants and utilities shared between HTTP and Async HTTP transports."""

from typing import Any
from urllib.parse import urljoin, urlsplit


def same_origin(url: str, location: str) -> bool:
    """Whether a redirect keeps the scheme, host, and effective port."""
    try:
        source = urlsplit(url)
        target = urlsplit(urljoin(url, location))
        if source.scheme not in ("http", "https") or target.scheme not in ("http", "https"):
            return False
        source_port = source.port if source.port is not None else (443 if source.scheme == "https" else 80)
        target_port = target.port if target.port is not None else (443 if target.scheme == "https" else 80)
        return (
            source.scheme == target.scheme
            and source.hostname is not None
            and source.hostname == target.hostname
            and source_port == target_port
        )
    except ValueError:
        return False


# Default retry configuration for HTTP transports
DEFAULT_RETRY_CONFIG: dict[str, Any] = {
    "total": 5,
    "read": 5,
    "connect": 5,
    "backoff_factor": 0.3,
    "status_forcelist": [500, 502, 503, 504],
    "allowed_methods": ["HEAD", "POST"],
}
