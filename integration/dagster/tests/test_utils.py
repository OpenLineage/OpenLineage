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
import uuid
from unittest.mock import patch

from dagster import EventRecordsFilter

from openlineage.dagster.utils import make_step_run_id, to_utc_iso_8601, make_step_job_name, \
    get_repository_name, get_event_log_records
from .conftest import make_pipeline_run_with_external_pipeline_origin


def test_to_utc_iso_8601():
    assert to_utc_iso_8601(1640995200) == "2022-01-01T00:00:00.000000Z"


def test_make_step_run_id():
    run_id = make_step_run_id()
    assert uuid.UUID(run_id).version == 4


def test_make_step_job_name():
    pipeline_name = "test_job"
    step_key = "test_graph.test_op"
    assert make_step_job_name(pipeline_name, step_key) == "test_job.test_graph.test_op"


@patch("openlineage.dagster.utils.DagsterInstance")
def test_get_event_log_records(mock_instance):
    last_storage_id = 100
    record_filter_limit = 100
    get_event_log_records(mock_instance, last_storage_id, record_filter_limit)
    mock_instance.get_event_records.assert_called_once_with(
        EventRecordsFilter(
            after_cursor=last_storage_id
        ),
        limit=record_filter_limit,
        ascending=True,
    )


@patch("openlineage.dagster.utils.DagsterInstance")
def test_get_repository_name(mock_instance):
    expected_repo = "test_repo"
    pipeline_run_id = str(uuid.uuid4())
    pipeline_run = make_pipeline_run_with_external_pipeline_origin(expected_repo)
    mock_instance.get_run_by_id.return_value = pipeline_run
    actual_repo = get_repository_name(mock_instance, pipeline_run_id)

    assert expected_repo == actual_repo
    mock_instance.get_run_by_id.assert_called_once_with(pipeline_run_id)
