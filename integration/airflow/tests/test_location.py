# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys
from unittest.mock import patch

import pytest
from .mocks.git_mock import execute_git_mock

from marquez_airflow.utils import get_location
log = logging.getLogger(__name__)


@patch('marquez_airflow.utils.execute_git',
       side_effect=execute_git_mock)
def test_dag_location(git_mock):
    assert ('https://github.com/MarquezProject/marquez/blob/'
            'abcd1234/integrations/airflow/tests/test_dags/'
            'test_dag.py' == get_location("tests/test_dags/test_dag.py"))


@patch('marquez_airflow.utils.execute_git',
       side_effect=execute_git_mock)
def test_bad_file_path(git_mock):
    log.debug("test_bad_file_path()")
    with pytest.raises(FileNotFoundError):
        # invalid file
        get_location("dags/missing-dag.py")


if __name__ == "__main__":
    pytest.main([sys.argv[0]])
