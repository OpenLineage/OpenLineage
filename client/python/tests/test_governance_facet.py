# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from dataclasses import asdict

from openlineage.client.facet import (
    DataMeshGovernanceDatasetFacet,
    PolicyEvaluationResult,
)


def test_governance_facet_initialization():
    facet = DataMeshGovernanceDatasetFacet(
        domain="core_banking",
        dataClassification="CONFIDENTIAL",
        piiColumns=["ssn", "account_number"],
    )
    assert facet.domain == "core_banking"
    assert facet.dataClassification == "CONFIDENTIAL"
    assert "ssn" in facet.piiColumns
    assert facet.is_compliant() is True


def test_governance_facet_policy_compliance_fail():
    fail_check = PolicyEvaluationResult(
        policyName="LINEAGE_RETENTION_RULE",
        engine="Rego",
        passed=False,
        severity="ERROR",
        details="Retention period must be >= 7 years",
    )
    facet = DataMeshGovernanceDatasetFacet(domain="risk", policyChecks=[fail_check])
    assert facet.is_compliant() is False


def test_governance_facet_serialization():
    check = PolicyEvaluationResult(
        policyName="OPA_PII_MASKING",
        engine="OPA",
        passed=True,
        evaluatedAt="2026-09-21T10:00:00Z",
    )
    facet = DataMeshGovernanceDatasetFacet(
        domain="payments",
        dataClassification="RESTRICTED",
        piiColumns=["routing_number"],
        policyChecks=[check],
    )
    payload = json.dumps(asdict(facet))
    data = json.loads(payload)

    assert data["domain"] == "payments"
    assert data["dataClassification"] == "RESTRICTED"
    assert data["policyChecks"][0]["policyName"] == "OPA_PII_MASKING"
    assert data["policyChecks"][0]["passed"] is True
    assert "_schemaURL" in data
