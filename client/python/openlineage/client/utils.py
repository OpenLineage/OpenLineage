# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import inspect
from typing import Type

import attr
import importlib
from warnings import warn


def import_from_string(path: str):
    try:
        module_path, target = path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, target)
    except Exception as e:
        raise ImportError(f"Failed to import {path}") from e


def try_import_from_string(path: str):
    try:
        return import_from_string(path)
    except ImportError as e:
        warn(e.msg)
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
