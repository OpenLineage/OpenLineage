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

Escaping is **disabled by default** and can be enabled by:

1. Setting the environment variable ``OPENLINEAGE__NAME__ESCAPING`` to
   ``true`` (case-insensitive), or
2. Setting ``name.escaping: true`` in the YAML configuration and calling
   :func:`configure` with the loaded value.

When both are provided the value passed to :func:`configure` takes
precedence over the environment variable.

Example::

    >>> from openlineage.client.naming.escape import escape, is_escaping_enabled
    >>> is_escaping_enabled()
    False
    >>> escape("mydb.example.com")
    'mydb.example.com'
"""

from __future__ import annotations

import os
from typing import Optional

_ENV_VAR = "OPENLINEAGE__NAME__ESCAPING"

# Config-derived override.  ``None`` means "no YAML config was loaded; fall
# back to the environment variable".  A non-``None`` value was set by
# :func:`configure` and takes precedence.
_config_override: Optional[bool] = None


def configure(escaping: Optional[bool]) -> None:
    """Apply the ``name.escaping`` value loaded from a YAML/dict config.

    Call this after the OpenLineage configuration has been parsed so that
    the ``name.escaping`` YAML field is honoured.  Pass ``None`` to clear the
    override and fall back to the environment variable.

    Args:
        escaping: ``True`` to enable escaping, ``False`` to disable it
            explicitly, or ``None`` to reset to env-var lookup.
    """
    global _config_override  # noqa: PLW0603
    _config_override = escaping


def is_escaping_enabled() -> bool:
    """Return ``True`` if dot-escaping is enabled.

    Resolution order:

    1. If :func:`configure` was called with a non-``None`` value, that value
       is returned.
    2. Otherwise the environment variable ``OPENLINEAGE__NAME__ESCAPING`` is
       consulted.
    """
    if _config_override is not None:
        return _config_override
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
