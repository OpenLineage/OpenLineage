# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os

import pytest

from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION
from mock import patch
log = logging.getLogger(__name__)

collect_ignore = []


if parse_version(AIRFLOW_VERSION) < parse_version("2.3.0"):
    collect_ignore.append("test_listener.py")

if parse_version(AIRFLOW_VERSION) < parse_version("2.2.4"):
    collect_ignore.append("extractors/test_redshift_sql_extractor.py")
    collect_ignore.append("extractors/test_s3_extractor.py")

if parse_version(AIRFLOW_VERSION) < parse_version("2.3.0"):
    collect_ignore.append("extractors/test_redshift_data_extractor.py")
    collect_ignore.append("extractors/test_sagemaker_extractors.py")


@pytest.fixture(scope="function")
def remove_redshift_conn():
    if 'REDSHIFT_CONN' in os.environ:
        del os.environ['REDSHIFT_CONN']
    if 'WRITE_SCHEMA' in os.environ:
        del os.environ['WRITE_SCHEMA']


@pytest.fixture(scope="function")
def we_module_env():
    os.environ['REDSHIFT_CONN'] = 'postgresql://user:password@host.io:1234/db'
    os.environ['WRITE_SCHEMA'] = 'testing'


@pytest.fixture(scope="function")
def dagbag():
    log.debug("dagbag()")
    os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = 'sqlite://'
    os.environ['MARQUEZ_NAMESPACE'] = 'test-marquez'

    from airflow import settings
    import airflow.utils.db as db_utils
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
            if k
            not in ["OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "OPENLINEAGE_EXTRACTORS"]
        },
        clear=True,
    ):
        yield
