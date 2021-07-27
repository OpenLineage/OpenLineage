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
import os

import pytest
log = logging.getLogger(__name__)


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
