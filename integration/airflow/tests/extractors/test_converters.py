# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest

from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version
if parse_version(AIRFLOW_VERSION) > parse_version("2.0.0"):
    pytestmark = pytest.mark.skip("Skipping tests for Airflow 2.0.0+")

from openlineage.airflow.extractors.converters import table_to_dataset


@pytest.mark.skipif(
    parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"),
    reason="requires AIRFLOW_VERSION to be higher than 2.0",
)
def test_table_to_dataset_conversion():
    from airflow.lineage.entities import Table
    t = Table(
        database="db",
        cluster="c",
        name="table1",
        tags="example"
    )

    d = table_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"
    assert d.facets["tags"] == "example"
