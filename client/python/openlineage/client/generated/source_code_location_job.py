# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar, Optional

from attr import define, field
from openlineage.client.generated.base import JobFacet


@define
class SourceCodeLocationJobFacet(JobFacet):
    type: str
    """the source control system"""

    url: str = field()
    """the full http URL to locate the file"""

    repoUrl: Optional[str] = field(default=None)  # noqa: N815
    """the URL to the repository"""

    path: Optional[str] = field(default=None)
    """the path in the repo containing the source files"""

    version: Optional[str] = field(default=None)
    """the current version deployed (not a branch name, the actual unique version)"""

    tag: Optional[str] = field(default=None)
    """optional tag name"""

    branch: Optional[str] = field(default=None)
    """optional branch name"""

    _additional_skip_redact: ClassVar[list[str]] = ["type", "url"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/SourceCodeLocationJobFacet.json#/$defs/SourceCodeLocationJobFacet"

    @url.validator
    def url_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        result = urlparse(value)
        if value and not all([result.scheme, result.netloc]):
            msg = "url is not a valid URI"
            raise ValueError(msg)
