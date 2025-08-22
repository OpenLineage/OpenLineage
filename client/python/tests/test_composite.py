# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import pytest
from openlineage.client.transport import ConsoleTransport
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
            "continue_on_success": False,
            "sort_transports": True,
        },
    )

    assert config.transports["kafka"]["config"]["bootstrap.servers"] == "localhost:9092"
    assert config.transports["kafka"]["topic"] == "random-topic"
    assert config.transports["kafka"]["messageKey"] == "key"
    assert config.transports["kafka"]["flush"] is False
    assert config.transports["console"] == {"type": "console"}
    assert config.continue_on_failure is False
    assert config.continue_on_success is False
    assert config.sort_transports is True


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
    assert config.continue_on_success is True
    assert config.sort_transports is False


@pytest.mark.parametrize("transports", [{}, []])
def test_empty_transports(transports):
    config = CompositeConfig(transports=transports, continue_on_failure=True, continue_on_success=True)
    with pytest.raises(ValueError, match="CompositeTransport initialization failed: No transports found"):
        CompositeTransport(config)


@mock.patch("openlineage.client.transport.get_default_factory")
def test_sort_transports(mock_factory):
    mock_transport1 = ConsoleTransport(None)
    mock_transport2 = ConsoleTransport(None)
    mock_transport2.priority = 2
    mock_transport3 = ConsoleTransport(None)
    mock_transport3.priority = 3

    # Configure mock factory
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2, mock_transport3]

    config = CompositeConfig(transports=[{}, {}, {}], sort_transports=True)
    transport = CompositeTransport(config)

    sorted_transports = [mock_transport3, mock_transport2, mock_transport1]
    assert transport.transports == sorted_transports


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
def test_emit_failure_no_continue_on_failure_with_error(mock_factory):
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
def test_emit_failure_no_continue_on_failure_without_error(mock_factory):
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


@pytest.mark.parametrize(
    ("continue_on_failure", "continue_on_success", "expected_calls", "should_raise"),
    [
        # 1) continue_on_failure=True, continue_on_success=True
        # Should emit to all transports even if one fails, never raises
        (True, True, [True, True, True], False),
        # 2) continue_on_failure=True, continue_on_success=False
        # Should emit until first success, ignore failures before that, never raises
        (True, False, [True, True, False], False),
        # 3) continue_on_failure=False, continue_on_success=True
        # Should raise on first failure immediately
        (False, True, [True, False, False], True),
        # 4) continue_on_failure=False, continue_on_success=False
        # Should stop on first success or failure; raises only if first emit fails
        (False, False, [True, False, False], True),
    ],
)
@mock.patch("openlineage.client.transport.get_default_factory")
def test_emit_continue_combinations(
    mock_factory, continue_on_failure, continue_on_success, expected_calls, should_raise
):
    # Setup transports: three transports with first failing, second succeeding
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport3 = MagicMock(spec=Transport)

    mock_transport1.emit.side_effect = Exception("First transport fails")
    mock_transport2.emit.side_effect = None
    mock_transport3.emit.side_effect = None

    # Configure mock factory
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2, mock_transport3]

    config = CompositeConfig(
        transports=[{}, {}, {}],  # mocked anyway
        continue_on_failure=continue_on_failure,
        continue_on_success=continue_on_success,
    )
    transport = CompositeTransport(config)
    event = MagicMock()

    if should_raise:
        with pytest.raises(RuntimeError):
            transport.emit(event)
    else:
        transport.emit(event)

    mock_emits = [mock_transport1.emit, mock_transport2.emit, mock_transport3.emit]
    for i, expected_success in enumerate(expected_calls):
        mock_emit = mock_emits[i]
        if expected_success:
            mock_emit.assert_called_once_with(event)
        else:
            mock_emit.assert_not_called()


@mock.patch("openlineage.client.transport.get_default_factory")
def test_emit_raises_error_when_all_failed(mock_factory):
    mock_transport1 = MagicMock(spec=Transport)
    mock_transport1.emit.side_effect = Exception("Error")
    mock_transport2 = MagicMock(spec=Transport)
    mock_transport2.emit.side_effect = Exception("Error")
    mock_factory.return_value.create.side_effect = [mock_transport1, mock_transport2]

    config = CompositeConfig(transports=[{}, {}], continue_on_failure=True)
    transport = CompositeTransport(config)
    event = MagicMock()

    with pytest.raises(RuntimeError, match="None of the transports successfully emitted the event"):
        transport.emit(event)


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
