# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0


import pytest
from openlineage.client import set_producer
from openlineage.common.provider.dbt.processor import (
    DbtArtifactProcessor,
    DbtRunContext,
)


@pytest.fixture(scope='session', autouse=True)
def setup_producer():
    set_producer('https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt')


def test_seed_snapshot_nodes_do_not_throw():
    processor = DbtArtifactProcessor(
        producer='https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt',
        job_namespace='job-namespace'
    )

    # Should just skip processing
    processor.parse_assertions(
        DbtRunContext({}, {"results": [{"unique_id": "seed.jaffle_shop.raw_orders"}]}), {}
    )
