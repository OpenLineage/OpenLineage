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
import json
import uuid
from unittest.mock import patch

from dagster import SensorDefinition, build_sensor_context, DagsterEventType
from dagster.core.test_utils import instance_for_test

from openlineage.dagster.cursor import OpenLineageCursor, RunningPipeline, RunningStep
from openlineage.dagster.sensor import openlineage_sensor
from .conftest import make_test_event_log_record


def test_basic_sensor_def():
    sensor_def = openlineage_sensor()
    assert isinstance(sensor_def, SensorDefinition)
    assert not sensor_def.targets


@patch("openlineage.dagster.sensor.get_event_log_records")
def test_cursor_update_with_after_storage_id(mock_event_log_records):
    with instance_for_test() as instance:
        context = build_sensor_context(instance=instance, repository_name="hello")
        openlineage_sensor(after_storage_id=100).evaluate_tick(context)

        assert context.cursor == json.dumps({
            "last_storage_id": 100,
            "running_pipelines": {}
        })


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_cursor_update_with_successful_run(mock_event_log_records, mock_step_run_id, mock_adapter):  # noqa: E501
    with instance_for_test() as instance:
        ol_sensor_def = openlineage_sensor(record_filter_limit=1)

        # 1. pipeline start
        pipeline_run_id = str(uuid.uuid4())
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_START, pipeline_run_id=pipeline_run_id
            )
        ]
        context = build_sensor_context(instance=instance)
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=1,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={},
                    repository_name=None
                )
            }
        )

        # 2. step start
        step_run_id = str(uuid.uuid4())
        step_key = "an_op"
        mock_step_run_id.return_value = step_run_id
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_START, pipeline_run_id=pipeline_run_id, step_key=step_key, storage_id=2  # noqa: E501
            )
        ]
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=2,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={
                        step_key: RunningStep(
                            step_run_id=step_run_id,
                            input_datasets=[],
                            output_datasets=[])
                    },
                    repository_name=None
                )
            }
        )

        # 3. step success
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_SUCCESS, pipeline_run_id=pipeline_run_id, step_key=step_key, storage_id=3  # noqa: E501
            )
        ]
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=3,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={},
                    repository_name=None
                )
            }
        )

        # 4. pipeline success
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_SUCCESS, pipeline_run_id=pipeline_run_id, storage_id=4
            )
        ]
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=4,
            running_pipelines={}
        )


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_cursor_update_with_failing_run(mock_event_log_records, mock_step_run_id, mock_adapter):
    with instance_for_test() as instance:
        ol_sensor_def = openlineage_sensor(record_filter_limit=1)

        # 1. pipeline start
        pipeline_run_id = str(uuid.uuid4())
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_START, pipeline_run_id=pipeline_run_id
            )
        ]
        context = build_sensor_context(instance=instance, cursor=None)
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=1,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={},
                    repository_name=None
                )
            },
        )

        # 2. step start
        step_run_id = str(uuid.uuid4())
        step_key = "an_op"
        mock_step_run_id.return_value = step_run_id
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_START, pipeline_run_id=pipeline_run_id, step_key=step_key, storage_id=2  # noqa: E501
            )
        ]
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=2,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={
                        step_key: RunningStep(
                            step_run_id=step_run_id,
                            input_datasets=[],
                            output_datasets=[])
                    },
                )
            }
        )

        # 3. step fail
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_FAILURE, pipeline_run_id=pipeline_run_id, step_key=step_key, storage_id=3  # noqa: E501
            )
        ]
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=3,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={},
                    repository_name=None
                )
            }
        )

        # 4. pipeline fail
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.RUN_SUCCESS, pipeline_run_id=pipeline_run_id, storage_id=4
            )
        ]
        ol_sensor_def.evaluate_tick(context)
        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=4,
            running_pipelines={}
        )


@patch("openlineage.dagster.sensor._ADAPTER")
@patch("openlineage.dagster.sensor.make_step_run_id")
@patch("openlineage.dagster.sensor.get_event_log_records")
def test_cursor_update_with_exception_raised(mock_event_log_records, mock_step_run_id, mock_adapter):  # noqa: E501
    with instance_for_test() as instance:
        pipeline_run_id = str(uuid.uuid4())
        step_key = "an_op"
        step_run_id = str(uuid.uuid4())
        mock_step_run_id.return_value = step_run_id
        mock_event_log_records.return_value = [
            make_test_event_log_record(
                DagsterEventType.STEP_START, pipeline_run_id=pipeline_run_id, step_key=step_key
            ),
            make_test_event_log_record(
                DagsterEventType.STEP_SUCCESS, pipeline_run_id=pipeline_run_id, step_key=step_key
            ),
        ]
        mock_adapter.complete_step.side_effect = Exception("test!")
        context = build_sensor_context(instance=instance)

        openlineage_sensor(record_filter_limit=2).evaluate_tick(context)

        assert OpenLineageCursor.from_json(context.cursor) == OpenLineageCursor(
            last_storage_id=1,
            running_pipelines={
                pipeline_run_id: RunningPipeline(
                    running_steps={
                        step_key: RunningStep(
                            step_run_id=step_run_id,
                            input_datasets=[],
                            output_datasets=[])
                    },
                    repository_name=None
                )
            }
        )
