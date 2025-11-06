# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

import attr
from openlineage.client.generated.base import JobFacet


@attr.define
class SourceCodeLocationJobFacet(JobFacet):
    type: str  # noqa: A003
    """
    the source control system

    Example: git|svn
    """
    url: str = attr.field()
    """
    the full http URL to locate the file

    Example: https://github.com/MarquezProject/marquez-airflow-
    quickstart/blob/693e35482bc2e526ced2b5f9f76ef83dec6ec691/dags/dummy_example.py
    """
    repoUrl: str | None = attr.field(default=None)  # noqa: N815
    """
    the URL to the repository

    Example: git@github.com:{org}/{repo}.git or
    https://github.com/{org}/{repo}.git|svn://<your_ip>/<repository_name>
    """
    path: str | None = attr.field(default=None)
    """
    the path in the repo containing the source files

    Example: path/to/my/dags
    """
    version: str | None = attr.field(default=None)
    """
    the current version deployed (not a branch name, the actual unique version)

    Example: git: the git sha | Svn: the revision number
    """
    tag: str | None = attr.field(default=None)
    """optional tag name"""

    branch: str | None = attr.field(default=None)
    """optional branch name"""

    _additional_skip_redact: ClassVar[list[str]] = ["type", "url"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/SourceCodeLocationJobFacet.json#/$defs/SourceCodeLocationJobFacet"

    @url.validator
    def url_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        urlparse(value)
