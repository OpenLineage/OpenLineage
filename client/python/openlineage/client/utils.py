# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import importlib
import inspect
import logging
from typing import List, Type

import attr

log = logging.getLogger(__name__)


def import_from_string(path: str):
    try:
        module_path, target = path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, target)
    except Exception as e:
        log.warning(e)
        raise ImportError(f"Failed to import {path}") from e


def try_import_from_string(path: str):
    try:
        return import_from_string(path)
    except ImportError:
        return None


def try_import_subclass_from_string(path: str, clazz: Type):
    subclass = try_import_from_string(path)
    if not inspect.isclass(subclass) or not issubclass(subclass, clazz):
        raise TypeError(
            f"Import path {path} - {str(subclass)} has to be class, and subclass of {str(clazz)}"
        )
    return subclass


# Filter dictionary to get only those key: value pairs that have
# key specified in passed attr class
def get_only_specified_fields(clazz, params: dict) -> dict:
    field_keys = [item.name for item in attr.fields(clazz)]
    return {
        key: value for key, value in params.items() if key in field_keys
    }


class RedactMixin:
    _skip_redact: List[str] = []

    @property
    def skip_redact(self) -> List[str]:
        return self._skip_redact
