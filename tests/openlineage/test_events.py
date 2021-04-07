import json
import os

import pytest

import openlineage.facet
from openlineage import run
from openlineage.run import Serde


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
                "nominalTime": openlineage.facet.NominalTimeRunFacet(
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
        producer="marquez-airflow"
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
    assert Serde.to_json(openlineage.facet.NominalTimeRunFacet(
        nominalStartTime='2020-01-01',
    )) == get_sorted_json("nominal_time_without_end.json")
