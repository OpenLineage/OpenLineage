# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, cast

import attr
from openlineage.client.generated.base import RunFacet
from openlineage.client.utils import RedactMixin


@attr.define
class AgentAttributionRunFacet(RunFacet):
    agentId: str  # noqa: N815
    """
    The identifier of the agent that executed this run. It is recommended to define this as a URN or
    DID. For example did:aps:z6Mk..., did:web:agent.example.com, agent:foo

    Example: did:aps:z6MkTestAgent001
    """
    covenantInEffect: CovenantInEffect  # noqa: N815
    """
    The governance attestation that was in force at execution time. The digest is the hash of the signed
    attestation bytes; the type indicates which governance framework emitted it. Consumers verify the
    digest against a resolvable attestation to confirm what the agent was allowed to do during this run.
    """
    accessedAt: str | None = attr.field(default=None)  # noqa: N815
    """The timestamp at which the agent accessed the input data source. ISO 8601 UTC."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-0/AgentAttributionRunFacet.json#/$defs/AgentAttributionRunFacet"

    @accessedAt.validator
    def accessedat_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from dateutil import parser

        parser.isoparse(value)
        if "t" not in value.lower():
            # make sure date-time contains time
            msg = f"Parsed date-time has to contain time: {value}"
            raise ValueError(msg)


@attr.define
class CovenantInEffect(RedactMixin):
    """
    The governance attestation that was in force at execution time. The digest is the hash of the signed
    attestation bytes; the type indicates which governance framework emitted it. Consumers verify the
    digest against a resolvable attestation to confirm what the agent was allowed to do during this run.
    """

    digest: str
    """
    Hash of the governance attestation that was in force at execution time. SHA-256 by default; the
    algorithm can be declared explicitly in the digestAlgorithm field or prefixed in the value (e.g.
    sha256:abc...).

    Example: sha256:3c9e9d2c6b7e8a1f4d5c6b7a8f9e0d1c2b3a4f5e6d7c8b9a0f1e2d3c4b5a6978
    """
    type: str  # noqa: A003
    """
    The governance signal type. Open enum; vendor-agnostic reference value is 'governance_attestation'.
    Implementers MAY emit a vendor-specific string as long as the digest remains a hash of a signed
    object that a compliance consumer can resolve and verify.

    Example: governance_attestation
    """
    digestAlgorithm: str | None = "sha-256"  # noqa: N815
    """
    The hash algorithm used to compute the digest. Defaults to sha-256 if omitted.

    Example: sha-256
    """
    resolver: str | None = attr.field(default=None)
    """
    Optional URL or DID of the signed attestation from which the digest is computed. When present, a
    compliance consumer MAY fetch the resolved object and verify the hash matches the digest. Absence of
    a resolver is acceptable; the digest alone is sufficient for tamper-evidence.

    Example: https://gateway.example.com/api/v1/public/trust/agent001
    """

    def with_additional_properties(self, **kwargs: Any) -> "CovenantInEffect":
        """Add additional properties to updated class instance."""
        current_attrs = [a.name for a in attr.fields(self.__class__)]

        new_class = attr.make_class(
            self.__class__.__name__,
            {k: attr.field(default=None) for k in kwargs if k not in current_attrs},
            bases=(self.__class__,),
        )
        new_class.__module__ = self.__class__.__module__
        attrs = attr.fields(self.__class__)
        for a in attrs:
            if not a.init:
                continue
            attr_name = a.name  # To deal with private attributes.
            init_name = a.alias
            if init_name not in kwargs:
                kwargs[init_name] = getattr(self, attr_name)
        return cast(CovenantInEffect, new_class(**kwargs))

    @resolver.validator
    def resolver_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        if value is None:
            return
        from urllib.parse import urlparse

        urlparse(value)
