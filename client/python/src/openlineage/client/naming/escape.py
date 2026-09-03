# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Dot-escaping utilities for OpenLineage name segments.

OpenLineage names are structured as dot-separated segments, e.g.
``{database}.{schema}.{table}``.  When a segment itself contains a literal
dot (e.g. an Oracle service name ``mydb.example.com``), the dot must be
escaped so that consumers can unambiguously split the name into its
constituent parts.

The escaping rule (from the naming specification) is:

    A literal ``.`` inside a segment is written as ``\\.``

Escaping is **disabled by default** and can be enabled by setting the
environment variable ``OPENLINEAGE__NAME__ESCAPING`` to ``true`` (case-
insensitive), or by setting ``name.escaping: true`` in the YAML configuration.

Example::

    >>> from openlineage.client.naming.escape import escape, is_escaping_enabled
    >>> is_escaping_enabled()
    False
    >>> escape("mydb.example.com")
    'mydb.example.com'
"""

from __future__ import annotations

import os

_ENV_VAR = "OPENLINEAGE__NAME__ESCAPING"


def is_escaping_enabled() -> bool:
    """Return ``True`` if dot-escaping is enabled.

    Escaping is **disabled by default**. It can be enabled by setting the
    environment variable ``OPENLINEAGE__NAME__ESCAPING=true`` (case-insensitive).
    """
    raw = os.environ.get(_ENV_VAR, "false")
    return raw.strip().lower() == "true"


def escape(segment: str) -> str:
    """Escape dots in a single name segment when escaping is enabled.

    A literal ``.`` is replaced with ``\\.`` so that consumers can tell
    structural dots (separating segments) from literal dots that are part
    of a segment value.

    The transformation is **only** applied when :func:`is_escaping_enabled`
    returns ``True``; otherwise the segment is returned unchanged.

    Args:
        segment: A single name component (e.g. database, schema, table).

    Returns:
        The segment with literal dots escaped (or unchanged if escaping is
        disabled).
    """
    if not is_escaping_enabled():
        return segment
    return segment.replace(".", "\\.")
