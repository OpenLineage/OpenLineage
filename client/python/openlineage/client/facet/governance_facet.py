# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class PolicyEvaluationResult:
    policyName: str
    engine: str
    passed: bool
    evaluatedAt: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    severity: str = "ERROR"
    details: Optional[str] = None


@dataclass
class DataMeshGovernanceDatasetFacet:
    domain: str
    owner: str
    _producer: str = "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DataMeshGovernanceDatasetFacet.json"
    _schemaURL: str = "https://openlineage.io/spec/facets/1-0-0/DataMeshGovernanceDatasetFacet.json"
    dataClassification: str = "INTERNAL"
    piiColumns: List[str] = field(default_factory=list)
    policyChecks: List[PolicyEvaluationResult] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)

    def is_compliant(self) -> bool:
        for check in self.policyChecks:
            if not check.passed and check.severity.upper() == "ERROR":
                return False
        return True
