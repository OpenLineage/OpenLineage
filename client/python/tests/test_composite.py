# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import pytest
from openlineage.client.transport.composite import CompositeConfig, CompositeTransport
from openlineage.client.transport.transport import Transport


def test_composite_loads_full_config() -> None:
    config = CompositeConfig.from_dict(
        {
            "type": "composite",
            "transports": {
                "kafka": {
                    "type": "kafka",
                    "config": {"bootstrap.servers": "localhost:9092"},
                    "topic": "random-topic",
                    "messageKey": "key",
                    "flush": False,
                },
                "console": {"type": "console"},
            },
            "continue_on_failure": False,
        },
    )

    assert config.transports["kafka"]["config"]["bootstrap.servers"] == "localhost:9092"
    assert config.transports["kafka"]["topic"] == "random-topic"
    assert config.transports["kafka"]["messageKey"] == "key"
    assert config.transports["kafka"]["flush"] is False
    assert config.transports["console"] == {"type": "console"}
    assert config.continue_on_failure is False


def test_composite_loads_partial_config_with_defaults() -> None:
    config = CompositeConfig.from_dict(
        {
            "type": "composite",
            "transports": [
                {
                    "type": "kafka",
                    "config": {"bootstrap.servers": "localhost:9092"},
                    "topic": "random-topic",
                    "messageKey": "key",
                    "flush": False,
                },
                {"type": "console"},
            ],
        },
    )
    assert config.transports[0]["config"]["bootstrap.servers"] == "localhost:9092"
    assert config.transports[0]["topic"] == "random-topic"
    assert config.transports[0]["messageKey"] == "key"
    assert config.transports[0]["flush"] is False
    assert config.transports[1] == {"type": "console"}
    assert config.continue_on_failure is True


def test_composite_transport_create_transports():
    config = CompositeConfig(transports=[{"type": "tests.transport.FakeTransport"}], continue_on_failure=True)
    transport = CompositeTransport(config)
    assert len(transport.transports) == 1
    assert transport.transports[0].kind == "fake"


def test_composite_transport_from_dict_config():
    config = CompositeConfig(
        transports={"fake_trans": {"type": "tests.transport.FakeTransport"}}, continue_on_failure=True
    )
    transport = CompositeTransport(config)
    assert len(transport.transports) == 1
    assert transport.transports[0].kind == "fake"
    assert transport.transports[0].name == "fake_trans"


@mock.patch("openlineage.client.transport.get_default_factory")
def test_transports(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)

    assert transport.transports == [mock_transport1, mock_transport2]


@mock.patch("openlineage.client.transport.get_default_factory")
def test_emit_success(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)
    event = MagicMock()

    transport.emit(event)

    mock_transport1.emit.assert_called_once_with(event)
    mock_transport2.emit.assert_called_once_with(event)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_emit_failure_no_continue_on_failure(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport2.emit.side_effect = Exception("Error")
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}], continue_on_failure=False)
    transport = CompositeTransport(config)
    event = MagicMock()

    with pytest.raises(RuntimeError):
        transport.emit(event)

    mock_transport1.emit.assert_called_once_with(event)
    mock_transport2.emit.assert_called_once_with(event)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_emit_failure_continue_on_failure(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}], continue_on_failure=False)
    transport = CompositeTransport(config)
    event = MagicMock()

    # Emit should not raise an exception because continue_on_failure is True.
    transport.emit(event)

    mock_transport1.emit.assert_called_once_with(event)
    mock_transport2.emit.assert_called_once_with(event)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_wait_for_completion_all_succeed(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport1.wait_for_completion.return_value = True
    mock_transport2.wait_for_completion.return_value = True
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)

    result = transport.wait_for_completion(5.0)

    assert result is True
    mock_transport1.wait_for_completion.assert_called_once_with(5.0)
    mock_transport2.wait_for_completion.assert_called_once_with(5.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_wait_for_completion_one_fails(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport1.wait_for_completion.return_value = True
    mock_transport2.wait_for_completion.return_value = False
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)

    result = transport.wait_for_completion(3.0)

    assert result is False
    mock_transport1.wait_for_completion.assert_called_once_with(3.0)
    mock_transport2.wait_for_completion.assert_called_once_with(3.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_wait_for_completion_default_timeout(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport1.wait_for_completion.return_value = True
    mock_transport2.wait_for_completion.return_value = True
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)

    result = transport.wait_for_completion()

    assert result is True
    mock_transport1.wait_for_completion.assert_called_once_with(-1.0)
    mock_transport2.wait_for_completion.assert_called_once_with(-1.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_close_all_succeed(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport1.close.return_value = True
    mock_transport2.close.return_value = True
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)

    result = transport.close(10.0)

    assert result is True
    mock_transport1.close.assert_called_once_with(10.0)
    mock_transport2.close.assert_called_once_with(10.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_close_with_exception_raises_last_exception(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport3 = MagicMock(spec=Transport)

    first_exception = ValueError("First transport error")
    last_exception = RuntimeError("Last transport error")

    mock_transport1.close.side_effect = first_exception
    mock_transport2.close.return_value = True
    mock_transport3.close.side_effect = last_exception
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2, mock_transport3]

    config = CompositeConfig(transports=[{}, {}, {}])
    transport = CompositeTransport(config)

    with pytest.raises(RuntimeError) as exc_info:
        transport.close(1.0)

    assert exc_info.value is last_exception
    mock_transport1.close.assert_called_once_with(1.0)
    mock_transport2.close.assert_called_once_with(1.0)
    mock_transport3.close.assert_called_once_with(1.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_close_continues_despite_exceptions(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport3 = MagicMock(spec=Transport)

    mock_transport1.close.side_effect = Exception("Transport 1 error")
    mock_transport2.close.return_value = True
    mock_transport3.close.return_value = False
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2, mock_transport3]

    config = CompositeConfig(transports=[{}, {}, {}])
    transport = CompositeTransport(config)

    with pytest.raises(Exception) as exc_info:
        transport.close()

    assert str(exc_info.value) == "Transport 1 error"
    # Verify all transports were attempted despite the first one failing
    mock_transport1.close.assert_called_once_with(-1.0)
    mock_transport2.close.assert_called_once_with(-1.0)
    mock_transport3.close.assert_called_once_with(-1.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_close_default_timeout(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport1.close.return_value = True
    mock_transport2.close.return_value = True
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}])
    transport = CompositeTransport(config)

    result = transport.close()

    assert result is True
    mock_transport1.close.assert_called_once_with(-1.0)
    mock_transport2.close.assert_called_once_with(-1.0)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_close_empty_transports(mock_factory):
    mock_factory.return_value.create.side_effect = []

    config = CompositeConfig(transports=[])
    transport = CompositeTransport(config)

    result = transport.close(5.0)

    assert result is True


@mock.patch("openlineage.client.transport.get_default_factory")
def test_wait_for_completion_empty_transports(mock_factory):
    mock_factory.return_value.create.side_effect = []

    config = CompositeConfig(transports=[])
    transport = CompositeTransport(config)

    result = transport.wait_for_completion(5.0)

    assert result is True
