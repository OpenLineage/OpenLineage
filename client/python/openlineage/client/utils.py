# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
import logging
from typing import Any, ClassVar, Type, cast

import attr

log = logging.getLogger(__name__)


def import_from_string(path: str) -> type[Any]:
    try:
        module_path, target = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return cast(Type[Any], getattr(module, target))
    except Exception as e:  # noqa: BLE001
        log.warning(e)
        msg = f"Failed to import {path}"
        raise ImportError(msg) from e


def try_import_from_string(path: str) -> type[Any] | None:
    try:
        return import_from_string(path)
    except ImportError:
        return None


# Filter dictionary to get only those key: value pairs that have
# key specified in passed attr class
def get_only_specified_fields(clazz: type[Any], params: dict[str, Any]) -> dict[str, Any]:
    field_keys = [item.name for item in attr.fields(clazz)]
    return {key: value for key, value in params.items() if key in field_keys}


class RedactMixin:
    _skip_redact: ClassVar[list[str]] = []

    @property
    def skip_redact(self) -> list[str]:
        return self._skip_redact
