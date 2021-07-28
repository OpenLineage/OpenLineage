import json
from enum import Enum
from unittest import mock

import attr

from openlineage.common.provider.dbt import DbtArtifactProcessor


def serialize(inst, field, value):
    if isinstance(value, Enum):
        return value.value
    return value


@mock.patch('uuid.uuid4')
def test_dbt_parse_small_event(mock_uuid):
    mock_uuid.side_effect = [
        '6edf42ed-d8d0-454a-b819-d09b9067ff99',
    ]

    processor = DbtArtifactProcessor('tests/dbt/small/dbt_project.yml')
    dbt_events = processor.parse()
    events = [
        attr.asdict(event, value_serializer=serialize)
        for event
        in dbt_events.starts + dbt_events.completes + dbt_events.fails
    ]
    with open('tests/dbt/small/result.json', 'r') as f:
        assert events == json.load(f)


@mock.patch('uuid.uuid4')
def test_dbt_parse_large_event(mock_uuid):
    mock_uuid.side_effect = [
        '6edf42ed-d8d0-454a-b819-d09b9067ff99',
        '1a69c0a7-04bb-408b-980e-cbbfb1831ef7',
        'f99310b4-339a-4381-ad3e-c1b95c24ff11',
        'c11f2efd-4415-45fc-8081-10d2aaa594d2',
        'ae0a988e-72ad-4caf-8223-fe9dcb923a3f'
    ]

    processor = DbtArtifactProcessor('tests/dbt/large/dbt_project.yml')
    dbt_events = processor.parse()
    events = [
        attr.asdict(event, value_serializer=serialize)
        for event
        in dbt_events.starts + dbt_events.completes + dbt_events.fails
    ]
    with open('tests/dbt/large/result.json', 'r') as f:
        assert events == json.load(f)


@mock.patch('uuid.uuid4')
@mock.patch('datetime.datetime')
def test_dbt_parse_failed_event(mock_datetime, mock_uuid):
    mock_datetime.now.return_value.isoformat.return_value = '2021-07-28T15:51:07.253587'
    mock_uuid.side_effect = [
        '6edf42ed-d8d0-454a-b819-d09b9067ff99',
        '1a69c0a7-04bb-408b-980e-cbbfb1831ef7',
        'f99310b4-339a-4381-ad3e-c1b95c24ff11',
        'c11f2efd-4415-45fc-8081-10d2aaa594d2',
        'ae0a988e-72ad-4caf-8223-fe9dcb923a3f'
    ]

    processor = DbtArtifactProcessor('tests/dbt/fail/dbt_project.yml')
    dbt_events = processor.parse()
    events = [
        attr.asdict(event, value_serializer=serialize)
        for event
        in dbt_events.starts + dbt_events.completes + dbt_events.fails
    ]
    with open('tests/dbt/fail/result.json', 'r') as f:
        assert events == json.load(f)
