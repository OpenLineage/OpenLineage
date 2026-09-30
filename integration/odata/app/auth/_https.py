# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Guard against sending credentials in cleartext."""

from __future__ import annotations

import httpx


def require_https(url: httpx.URL | str) -> None:
    """Raise ``ValueError`` unless ``url`` uses the ``https`` scheme.

    Called before attaching credentials to any request, including absolute metadata/data URLs and
    ``@odata.nextLink`` values supplied by the server rather than by configuration.
    """
    scheme = httpx.URL(str(url)).scheme
    if scheme != "https":
        raise ValueError(f"Refusing to send credentials over non-HTTPS URL: {url}")
