# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import attr

from openlineage.client.facet import BaseFacet

@attr.define
class PrefectDeploymentRunFacet(BaseFacet):
    deployment_id: str
    created: str
    updated: str
    name: str
    def __init__(self, deployment_id, created, updated, name):
        super().__init__()
        self.deployment_id = deployment_id
        self.created = created
        self.updated = updated
        self.name = name
    
    @staticmethod
    def _get_schema() -> str:
        return "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet" #TODO

    @staticmethod
    def _get_producer() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/0.18.0/client/python"
