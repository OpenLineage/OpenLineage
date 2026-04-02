# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
from unittest.mock import patch

from openlineage.client.serde import Serde
from openlineage.common.provider.dbt.facets import DbtNodeJobFacet, DbtRunRunFacet, DbtVersionRunFacet
from openlineage.common.provider.dbt.utils import get_ci_pr_number


def test_facet_copy_serialization_dbt_version_run_facet():
    facet = DbtVersionRunFacet(version="version", producer="producer")
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_dbt_run_run_facet():
    facet = DbtRunRunFacet(
        invocation_id="invocation_id", project_name="project_name", dbt_runtime="core", producer="producer"
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


def test_facet_copy_serialization_dbt_node_job_facet():
    facet = DbtNodeJobFacet(
        original_file_path="models/staging/stg_customers.sql",
        database="analytics",
        schema="staging",
        alias="stg_customers",
        unique_id="model.my_project.stg_customers",
        producer="producer",
    )
    facet_copy = copy.deepcopy(facet)
    assert Serde.to_json(facet) == Serde.to_json(facet_copy)


class TestGetCiPrNumber:
    @patch.dict("os.environ", {"GITHUB_REF": "refs/pull/42/merge"}, clear=True)
    def test_github_actions_pull_request(self):
        assert get_ci_pr_number() == "42"

    @patch.dict("os.environ", {"GITHUB_REF": "refs/heads/main"}, clear=True)
    def test_github_actions_push_returns_none(self):
        assert get_ci_pr_number() is None

    @patch.dict("os.environ", {"CI_MERGE_REQUEST_IID": "123"}, clear=True)
    def test_gitlab_ci_merge_request(self):
        assert get_ci_pr_number() == "123"

    @patch.dict("os.environ", {}, clear=True)
    def test_no_ci_env_vars_returns_none(self):
        assert get_ci_pr_number() is None

    @patch.dict("os.environ", {"CI_MERGE_REQUEST_IID": "99", "GITHUB_REF": "refs/pull/42/merge"}, clear=True)
    def test_gitlab_takes_precedence_over_github(self):
        assert get_ci_pr_number() == "99"
