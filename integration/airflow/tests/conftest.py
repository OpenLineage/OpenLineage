# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from unittest.mock import patch

import pytest

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def remove_redshift_conn():
    if "REDSHIFT_CONN" in os.environ:
        del os.environ["REDSHIFT_CONN"]
    if "WRITE_SCHEMA" in os.environ:
        del os.environ["WRITE_SCHEMA"]


@pytest.fixture(scope="function")
def we_module_env():
    os.environ["REDSHIFT_CONN"] = "postgresql://user:password@host.io:1234/db"
    os.environ["WRITE_SCHEMA"] = "testing"


@pytest.fixture(scope="function")
def dagbag():
    log.debug("dagbag()")
    os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite://"
    os.environ["MARQUEZ_NAMESPACE"] = "test-marquez"

    import airflow.utils.db as db_utils
    from airflow import settings

    db_utils.resetdb(settings.RBAC)
    from airflow.models import DagBag

    dagbag = DagBag(include_examples=False)
    return dagbag


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with patch.dict(
        os.environ,
        {
            k: v
            for k, v in os.environ.items()
            if k not in ["OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "OPENLINEAGE_EXTRACTORS"]
        },
        clear=True,
    ):
        yield
