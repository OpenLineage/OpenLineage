# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import sys
from unittest.mock import MagicMock

# Mock openlineage_sql before importing processor
sys.modules["openlineage_sql"] = MagicMock()

import unittest  # noqa: E402

from openlineage.client.facet_v2 import ownership_dataset  # noqa: E402
from openlineage.common.provider.dbt.processor import DbtArtifactProcessor, ModelNode  # noqa: E402


class TestDbtOwnership(unittest.TestCase):
    def setUp(self):
        self.processor = DbtArtifactProcessor(
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
            job_namespace="job-namespace",
        )

    def test_node_to_output_dataset_with_ownership(self):
        node = ModelNode(
            type="model",
            metadata_node={
                "database": "db",
                "schema": "schema",
                "alias": "alias",
                "name": "model_name",
                "description": "description",
                "columns": {},
                "meta": {"owner": "data_team"},
            },
        )

        dataset = self.processor.node_to_output_dataset(node, has_facets=True)

        self.assertIn("ownership", dataset.facets)
        ownership_facet = dataset.facets["ownership"]
        self.assertIsInstance(ownership_facet, ownership_dataset.OwnershipDatasetFacet)
        self.assertEqual(len(ownership_facet.owners), 1)
        self.assertEqual(ownership_facet.owners[0].name, "data_team")

    def test_node_to_output_dataset_without_ownership(self):
        node = ModelNode(
            type="model",
            metadata_node={
                "database": "db",
                "schema": "schema",
                "alias": "alias",
                "name": "model_name",
                "description": "description",
                "columns": {},
                "meta": {},
            },
        )

        dataset = self.processor.node_to_output_dataset(node, has_facets=True)

        self.assertNotIn("ownership", dataset.facets)


if __name__ == "__main__":
    unittest.main()
