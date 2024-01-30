# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import re
import sys
from unittest.mock import patch

import pytest
from openlineage.airflow.utils import get_location

from tests.mocks.git_mock import execute_git_mock

log = logging.getLogger(__name__)


@patch("openlineage.airflow.utils.execute_git", side_effect=execute_git_mock)
def test_dag_location(git_mock):
    assert re.match(
        r"https://github.com/[^/]+/OpenLineage/blob/"
        "abcd1234/integration/airflow/tests/test_dags/test_dag.py",
        get_location("tests/test_dags/test_dag.py"),
    )


@patch("openlineage.airflow.utils.execute_git", side_effect=execute_git_mock)
def test_bad_file_path(git_mock):
    log.debug("test_bad_file_path()")
    with pytest.raises(FileNotFoundError):
        # invalid file
        get_location("dags/missing-dag.py")


if __name__ == "__main__":
    pytest.main([sys.argv[0]])
