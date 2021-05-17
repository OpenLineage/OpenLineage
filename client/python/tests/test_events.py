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
import os

import pytest

from openlineage.serde import Serde
from openlineage import run, facet


def get_sorted_json(file_name: str) -> str:
    dirpath = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dirpath, file_name), 'r') as f:
        loaded = json.load(f)
        return json.dumps(loaded, sort_keys=True)


def test_full_core_event_serializes_properly():
    runEvent = run.RunEvent(
        eventType=run.RunState.START,
        eventTime='2020-02-01',
        run=run.Run(
            runId='1500100900',
            facets={
                "nominalTime": facet.NominalTimeRunFacet(
                    nominalStartTime='2020-01-01',
                    nominalEndTime='2020-01-02'
                )
            }
        ),
        job=run.Job(
            namespace="openlineage",
            name="name",
            facets={}
        ),
        inputs=[],
        outputs=[],
        producer="openlineage-python"
    )

    assert Serde.to_json(runEvent) == get_sorted_json('serde_example.json')


def test_run_event_type_validated():
    with pytest.raises(ValueError):
        run.RunEvent(
            "asdf",
            "2020-02-01",
            run.Run("1500", {}),
            run.Job("default", "name"),
            "producer"
        )

    # TODO: validate dates
    # with pytest.raises(ValueError):
    #     events.RunEvent(events.RunState.START, 1500, events.Run("1500", {}))


def test_nominal_time_facet_does_not_require_end_time():
    assert Serde.to_json(facet.NominalTimeRunFacet(
        nominalStartTime='2020-01-01',
    )) == get_sorted_json("nominal_time_without_end.json")


def test_schema_field_default():
    assert Serde.to_json(facet.SchemaField(name='asdf', type='int4')) == \
           '{"name": "asdf", "type": "int4"}'

    assert Serde.to_json(facet.SchemaField(name='asdf', type='int4', description='primary key')) == \
           '{"description": "primary key", "name": "asdf", "type": "int4"}'
