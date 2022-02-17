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
import tempfile
import time
import uuid
from unittest import mock
from unittest.mock import patch, call

from dagster import job, op, execute_pipeline, reconstructable, build_sensor_context, DagsterEventType  # noqa: E501
from dagster.core.test_utils import instance_for_test

from openlineage.dagster.sensor import openlineage_sensor
from .conftest import make_test_event_log_record


@job
def a_job():

    @op
    def an_op():
        pass

    an_op()


@patch("openlineage.dagster.sensor.get_repository_name")
@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor._ADAPTER")
def test_sensor_with_complete_job_run_and_repository(mock_adapter, mock_step_run_id, mock_get_repository_name):  # noqa: E501
    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(
                temp_dir=temp_dir,
                overrides={  # to avoid run-sharded event log storage warning
                    "event_log_storage": {
                        "module": "dagster.core.storage.event_log",
                        "class": "ConsolidatedSqliteEventLogStorage",
                        "config": {
                            "base_dir": temp_dir
                        }
                    },
                },
        ) as instance:
            repository_name = "a_repository"
            pipeline_name = "a_job"
            step_key = "an_op"
            step_run_id = str(uuid.uuid4())
            mock_step_run_id.return_value = step_run_id
            mock_get_repository_name.return_value = repository_name

            result = execute_pipeline(
                pipeline=reconstructable(a_job),
                instance=instance,
                raise_on_error=False
            )
            pipeline_run_id = result.run_id

            context = build_sensor_context(instance=instance)
            previous_cursor = None
            while True:
                openlineage_sensor().evaluate_tick(context)
                if context.cursor == previous_cursor:
                    break
                else:
                    previous_cursor = context.cursor

            mock_adapter.assert_has_calls(
                [
                    call.start_pipeline(pipeline_name, pipeline_run_id, mock.ANY, repository_name),  # noqa: E501
                    call.start_step(pipeline_name, pipeline_run_id, mock.ANY, step_run_id, step_key, repository_name),  # noqa: E501
                    call.complete_step(pipeline_name, pipeline_run_id, mock.ANY, step_run_id, step_key, repository_name),  # noqa: E501
                    call.complete_pipeline(pipeline_name, pipeline_run_id, mock.ANY, repository_name)  # noqa: E501
                ]
            )


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_start_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        mock_event_log_records.return_value = [make_test_event_log_record(
            DagsterEventType.RUN_START, pipeline_name, pipeline_run_id, timestamp
        )]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.start_pipeline(pipeline_name, pipeline_run_id, timestamp, None)
        ])


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_complete_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        mock_event_log_records.return_value = [make_test_event_log_record(
            DagsterEventType.RUN_SUCCESS, pipeline_name, pipeline_run_id, timestamp
        )]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.complete_pipeline(pipeline_name, pipeline_run_id, timestamp, None)
        ])


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_fail_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        mock_event_log_records.return_value = [make_test_event_log_record(
            DagsterEventType.RUN_FAILURE, pipeline_name, pipeline_run_id, timestamp
        )]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.fail_pipeline(pipeline_name, pipeline_run_id, timestamp, None)
        ])


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_cancel_pipeline(mock_event_log_records, mock_adapter):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        mock_event_log_records.return_value = [make_test_event_log_record(
            DagsterEventType.RUN_CANCELED, pipeline_name, pipeline_run_id, timestamp
        )]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.cancel_pipeline(pipeline_name, pipeline_run_id, timestamp, None)
        ])


@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_start_step(mock_event_log_records, mock_adapter, mock_new_step_run_id):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        step_run_id = str(uuid.uuid4())
        step_key = "an_op"
        mock_event_log_records.return_value = [make_test_event_log_record(
            DagsterEventType.STEP_START, pipeline_name, pipeline_run_id, timestamp, step_key
        )]
        mock_new_step_run_id.return_value = step_run_id

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.start_step(
                pipeline_name, pipeline_run_id, timestamp, step_run_id, step_key, None
            )
        ])


@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_complete_step(mock_event_log_records, mock_adapter, mock_new_step_run_id):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        step_run_id = str(uuid.uuid4())
        step_key = "an_op"
        mock_new_step_run_id.return_value = step_run_id
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_SUCCESS, pipeline_name, pipeline_run_id, timestamp, step_key
            )
        ]

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.complete_step(
                pipeline_name, pipeline_run_id, timestamp, step_run_id, step_key, None
            )
        ])


@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_sensor_fail_step(mock_event_log_records, mock_adapter, mock_new_step_run_id):
    with instance_for_test() as instance:
        pipeline_name = "a_job"
        pipeline_run_id = str(uuid.uuid4())
        timestamp = time.time()
        step_run_id = str(uuid.uuid4())
        step_key = "an_op"
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_FAILURE, pipeline_name, pipeline_run_id, timestamp, step_key
            )
        ]
        mock_new_step_run_id.return_value = step_run_id

        context = build_sensor_context(instance=instance)
        openlineage_sensor().evaluate_tick(context)

        mock_adapter.assert_has_calls([
            mock.call.fail_step(
                pipeline_name, pipeline_run_id, timestamp, step_run_id, step_key, None
            )
        ])
