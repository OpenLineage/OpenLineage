# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import attr
from openlineage.client.facet_v2 import RunFacet


@attr.define
class PrefectDeploymentRunFacet(RunFacet):
    deploymentId: str = attr.field(alias="deployment_id")  # NOQA: N815
    created: str
    updated: str
    name: str

    @staticmethod
    def _get_schema() -> str:
        return "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/integration/prefect/src/openlineage_prefect/prefect_adapter/facets/PrefectDeploymentRunFacet.json#/$defs/PrefectDeploymentRunFacet"
