# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import re
import subprocess
import sys
from unittest.mock import patch

import pytest
from openlineage.airflow.utils import get_location

log = logging.getLogger(__name__)


def execute_git_mock(cwd, params):
    # just mock the git revision
    log.debug("execute_git_mock()")
    if len(cwd) > 0 and params[0] == "rev-list":
        return "abcd1234"

    p = subprocess.Popen(["git"] + params, cwd=cwd, stdout=subprocess.PIPE, stderr=None)
    p.wait(timeout=0.5)
    out, err = p.communicate()
    return out.decode("utf8").strip()


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
