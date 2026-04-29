---
sidebar_position: 11
---

# Agent Attribution Facet

The `AgentAttributionRunFacet` captures information about the autonomous agent that executed a run, along with the governance attestation (covenant) that was in force at execution time. Agent-produced lineage events need two run-scoped properties that existing facets don't cover cleanly: the agent's identifier and a tamper-evident reference to the policy the agent was operating under.

- `agentId`: The identifier of the agent that executed this run. Recommended as a URN or DID (e.g. `did:example:agent:001`, `did:web:agent.example.com`).
- `accessedAt`: The timestamp at which the agent accessed the input data source.
- `covenantInEffect`: The governance attestation that was in force at execution time, carried as a digest so compliance consumers can verify what the agent was authorized to do during this run.
  - `digest` (required): Hash of the signed attestation bytes. SHA-256 by default; the algorithm can be declared explicitly in `digestAlgorithm` or prefixed in the value (e.g. `sha256:abc...`).
  - `digestAlgorithm` (optional, default `sha-256`): The hash algorithm used to compute the digest.
  - `type` (required): The governance signal type. Open enum; the vendor-agnostic reference value is `governance_attestation`. Implementations MAY emit a vendor-specific string as long as the digest remains a hash of a signed object a compliance consumer can resolve.
  - `resolver` (optional): URL or DID of the signed attestation from which the digest is computed. Absence is acceptable; the digest alone is sufficient for tamper-evidence (supporting air-gapped or non-public compliance stores).

`covenantInEffect` allows `additionalProperties: true` scoped to the sub-object, so implementations can add vendor-specific fields (enforcement outcome, reason codes, bilateral receipt hashes, signatures, signer key ID, reviewer chain, etc.) without needing a facet version bump.

Example:

```json
{
    ...
    "run": {
        "facets": {
            "agentAttribution": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/main/client",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/AgentAttributionRunFacet.json",
                "agentId": "did:example:agent:001",
                "accessedAt": "2026-04-21T15:44:22.000Z",
                "covenantInEffect": {
                    "digest": "sha256:3c9e9d2c6b7e8a1f4d5c6b7a8f9e0d1c2b3a4f5e6d7c8b9a0f1e2d3c4b5a6978",
                    "digestAlgorithm": "sha-256",
                    "type": "governance_attestation",
                    "resolver": "https://example.com/api/v1/public/trust/agent001"
                }
            }
        }
    }
    ...
}
```

Example with vendor extensions (bilateral receipt — pre-execution authorization hash + post-execution result hash, both signed by the same agent key):

```json
{
    ...
    "run": {
        "facets": {
            "agentAttribution": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/main/client",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/AgentAttributionRunFacet.json",
                "agentId": "did:example:agent:scribe-42",
                "accessedAt": "2026-04-21T14:29:12.000Z",
                "covenantInEffect": {
                    "digest": "sha256:7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
                    "type": "vendor_specific_covenant",
                    "resolver": "https://example.com/api/v1/covenants/cov-a8f3b2",
                    "authorizationHash": "sha256:0a5bc9a9941286da6bb365d6a25ea5412fa014583e2b99cb1ccc92af556f580a",
                    "authorizationSignature": "APRQxuV0SOorX-Bol50NWtLdltqbw_uhFAyKs-i1WhhNUZdssauZkuDrhykQG2_i53dVybydSQc06dRt2EBQBQ",
                    "resultHash": "sha256:0fbdb588d8d7f25af779424b9238628fd25489af125a446e4daeb7257ec555ea",
                    "resultSignature": "R-yd0OJxVB01-cnSoDir-SeGOt3WBsb57OyZgBMzt_KxNDij-NDFANSvEBRDuCKjo1N3cv4zyh6-9w3zeGhACw",
                    "signerKeyId": "did:example:agent:scribe-42#key-ed25519-1"
                }
            }
        }
    }
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/AgentAttributionRunFacet.json).
