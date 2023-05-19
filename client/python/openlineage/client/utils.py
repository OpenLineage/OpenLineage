# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
import inspect
import logging
from typing import Any, Type, cast

import attr

log = logging.getLogger(__name__)


def import_from_string(path: str) -> type[Any]:
    try:
        module_path, target = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, target)
    except Exception as e:  # noqa: BLE001
        log.warning(e)
        msg = f"Failed to import {path}"
        raise ImportError(msg) from e


def try_import_from_string(path: str) -> type[Any] | None:
    try:
        return import_from_string(path)
    except ImportError:
        return None


def try_import_subclass_from_string(path: str, clazz: type[Any]) -> type[Any]:
    subclass = try_import_from_string(path)
    if not inspect.isclass(subclass) or not issubclass(subclass, clazz):
        msg = f"Import path {path} - {subclass!s} has to be class, and subclass of {clazz!s}"
        raise TypeError(msg)
    return cast(Type[Any], subclass)


# Filter dictionary to get only those key: value pairs that have
# key specified in passed attr class
def get_only_specified_fields(clazz: type[Any], params: dict[str, Any]) -> dict[str, Any]:
    field_keys = [item.name for item in attr.fields(clazz)]
    return {key: value for key, value in params.items() if key in field_keys}


class RedactMixin:
    _skip_redact: list[str] = []

    @property
    def skip_redact(self) -> list[str]:
        return self._skip_redact
