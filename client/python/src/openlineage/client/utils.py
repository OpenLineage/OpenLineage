# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
import logging
import os
import subprocess
from typing import Any, ClassVar, cast

import attr

log = logging.getLogger(__name__)


def import_from_string(path: str) -> type[Any]:
    try:
        module_path, target = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return cast("type[Any]", getattr(module, target))
    except Exception as e:
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


def deep_merge_dicts(dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
    """Deep merges two dictionaries.

    This function merges two dictionaries while handling nested dictionaries.
    For keys that exist in both dictionaries, the values from dict2 take precedence.
    If a key exists in both dictionaries and the values are dictionaries themselves,
    they are merged recursively.
    This function merges only dictionaries. If key is of different type, e.g. list
    it does not work properly.
    """
    merged = dict1.copy()
    for k, v in dict2.items():
        if k in merged and isinstance(v, dict):
            merged[k] = deep_merge_dicts(merged.get(k, {}), v)
        else:
            merged[k] = v
    return merged


def _run_git_command(args: list[str], timeout: int = 5) -> str | None:
    """Run a git command and return stripped stdout, or None on any failure."""
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=True,
            cwd=os.getcwd(),
            timeout=timeout,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def get_git_repo_url(repo_url: str | None = None, timeout: int = 5) -> str | None:
    """Return a repo URL suitable for SourceCodeLocationJobFacet.

    If *repo_url* is given it is used directly; otherwise the URL is
    auto-detected from ``git remote get-url origin`` in the current
    working directory.  The URL is returned as-is with no normalization.
    """
    if repo_url:
        return repo_url
    return _run_git_command(["git", "remote", "get-url", "origin"], timeout)


def get_git_version(version: str | None = None, timeout: int = 5) -> str | None:
    """Return the current git commit SHA, or *version* if explicitly provided."""
    if version:
        return version
    return _run_git_command(["git", "rev-parse", "HEAD"], timeout)


def get_git_branch(branch: str | None = None, timeout: int = 5) -> str | None:
    """Return the current git branch name, or *branch* if explicitly provided.

    Returns None in detached HEAD state (e.g. during CI checkout).
    """
    if branch:
        return branch
    result = _run_git_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], timeout)
    return None if result == "HEAD" else result


def get_git_tag(tag: str | None = None, timeout: int = 5) -> str | None:
    """Return the exact tag on HEAD if one exists, or *tag* if explicitly provided.

    Returns None if HEAD has no exact tag — partial ``git describe`` output
    (e.g. ``v1.2.3-4-gabcdef``) is intentionally excluded.
    """
    if tag:
        return tag
    return _run_git_command(["git", "describe", "--tags", "--exact-match", "HEAD"], timeout)


class RedactMixin:
    _skip_redact: ClassVar[list[str]] = []

    @property
    def skip_redact(self) -> list[str]:
        return self._skip_redact
