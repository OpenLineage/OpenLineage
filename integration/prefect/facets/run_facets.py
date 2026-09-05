# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import attr
from openlineage.client.facet import BaseFacet


@attr.define
class PrefectDeploymentRunFacet(BaseFacet):
    deploymentId: str
    created: str
    updated: str
    name: str

    def __init__(self, deploymentId, created, updated, name):
        super().__init__()
        self.deploymentId = deploymentId
        self.created = created
        self.updated = updated
        self.name = name

    @staticmethod
    def _get_schema() -> str:
        return "https://raw.githubusercontent.com/OpenLineage/openlineage/integration/prefect/facets/PrefectDeploymentRunFacet.json"

    @staticmethod
    def _get_producer() -> str:
        return (
            "https://github.com/OpenLineage/openlineage/integration/prefect"
        )
