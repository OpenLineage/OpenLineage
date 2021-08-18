import json
from enum import Enum
from unittest import mock

import attr
import pytest
import os

from openlineage.common.provider.dbt import DbtArtifactProcessor
from openlineage.client import set_producer


@pytest.fixture(scope='session', autouse=True)
def setup_producer():
    set_producer('https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt')
    os.environ['OPENLINEAGE_NAMESPACE'] = 'test-namespace'


def serialize(inst, field, value):
    if isinstance(value, Enum):
        return value.value
    return value


@mock.patch('uuid.uuid4')
def test_dbt_parse_small_event(mock_uuid):
    mock_uuid.side_effect = [
        '6edf42ed-d8d0-454a-b819-d09b9067ff99',
    ]

    processor = DbtArtifactProcessor(
        producer='https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt',
        project_dir='tests/dbt/small'
    )
    dbt_events = processor.parse()
    events = [
        attr.asdict(event, value_serializer=serialize)
        for event
        in dbt_events.starts + dbt_events.completes + dbt_events.fails
    ]
    with open('tests/dbt/small/result.json', 'r') as f:
        assert events == json.load(f)


@mock.patch('uuid.uuid4')
def test_dbt_parse_catalog_event(mock_uuid):
    mock_uuid.side_effect = [
        '6edf42ed-d8d0-454a-b819-d09b9067ff99',
    ]

    processor = DbtArtifactProcessor(
        producer='https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt',
        project_dir='tests/dbt/catalog'
    )
    dbt_events = processor.parse()
    events = [
        attr.asdict(event, value_serializer=serialize)
        for event
        in dbt_events.starts + dbt_events.completes + dbt_events.fails
    ]
    with open('tests/dbt/catalog/result.json', 'r') as f:
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

    processor = DbtArtifactProcessor(
        producer='https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt',
        project_dir='tests/dbt/large'
    )
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
    mock_datetime.now.return_value.isoformat.return_value = '2021-07-28T13:10:51.245287+00:00'
    mock_uuid.side_effect = [
        '6edf42ed-d8d0-454a-b819-d09b9067ff99',
        '1a69c0a7-04bb-408b-980e-cbbfb1831ef7',
        'f99310b4-339a-4381-ad3e-c1b95c24ff11',
        'c11f2efd-4415-45fc-8081-10d2aaa594d2',
        'ae0a988e-72ad-4caf-8223-fe9dcb923a3f'
    ]

    processor = DbtArtifactProcessor(
        producer='https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt',
        project_dir='tests/dbt/fail'
    )
    dbt_events = processor.parse()
    events = [
        attr.asdict(event, value_serializer=serialize)
        for event
        in dbt_events.starts + dbt_events.completes + dbt_events.fails
    ]
    with open('tests/dbt/fail/result.json', 'r') as f:
        assert events == json.load(f)
