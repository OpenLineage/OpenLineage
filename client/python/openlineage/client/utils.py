# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
import logging
import os
from collections import defaultdict
from typing import Any, Type, cast

import attr
import yaml

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
    _skip_redact: list[str] = []

    @property
    def skip_redact(self) -> list[str]:
        return self._skip_redact


def load_config() -> dict[str, Any]:
    file = _find_yaml()
    if file:
        try:
            with open(file) as f:
                config: dict[str, Any] = yaml.safe_load(f)
                return config
        except Exception:  # noqa: BLE001, S110
            # Just move to read env vars
            pass
    return defaultdict(dict)


def _find_yaml() -> str | None:
    # Check OPENLINEAGE_CONFIG env variable
    path = os.getenv("OPENLINEAGE_CONFIG", None)
    try:
        if path and os.path.isfile(path) and os.access(path, os.R_OK):
            return path
    except Exception:  # noqa: BLE001
        if path:
            log.exception("Couldn't read file %s: ", path)
        else:
            pass  # We can get different errors depending on system

    # Check current working directory:
    try:
        cwd = os.getcwd()
        if "openlineage.yml" in os.listdir(cwd):
            return os.path.join(cwd, "openlineage.yml")
    except Exception:  # noqa: BLE001, S110
        pass  # We can get different errors depending on system

    # Check $HOME/.openlineage dir
    try:
        path = os.path.expanduser("~/.openlineage")
        if "openlineage.yml" in os.listdir(path):
            return os.path.join(path, "openlineage.yml")
    except Exception:  # noqa: BLE001, S110
        # We can get different errors depending on system
        pass
    return None
