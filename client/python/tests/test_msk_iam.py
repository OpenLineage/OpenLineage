# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import os
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from openlineage.client.run import Job, Run, RunEvent, RunState
from openlineage.client.transport.msk_iam import (
    MSKIAMConfig,
    MSKIAMTransport,
    _detect_running_region,
    _oauth_cb,
)
from openlineage.client.uuid import generate_new_uuid

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture()
def event() -> RunEvent:
    return RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="kafka", name="test"),
        producer="prod",
        schemaURL="schema",
    )


def test_msk_loads_full_config() -> None:
    config = MSKIAMConfig.from_dict(
        {
            "type": "msk",
            "config": {"bootstrap.servers": "xxx.c2.kafka.us-east-1.amazonaws.com:9098"},
            "topic": "random-topic",
            "flush": False,
            "region": "us-east-1",
        },
    )

    assert config.config["bootstrap.servers"] == "xxx.c2.kafka.us-east-1.amazonaws.com:9098"
    assert config.topic == "random-topic"
    assert config.region == "us-east-1"
    assert config.flush is False


@mock.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "eu-west-1"})
def test_msk_detect_running_default_region() -> None:
    region = _detect_running_region()

    assert region == "eu-west-1"


@mock.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "eu-central-1"})
def test_msk_detect_running_region() -> None:
    region = _detect_running_region()

    assert region == "eu-central-1"


def test_msk_detect_running_region_empty(mocker: MockerFixture) -> None:
    mocker.patch("requests.get", side_effect=Exception())
    region = _detect_running_region()
    assert region is None


def test_msk_load_config_fails_on_no_config() -> None:
    with pytest.raises(TypeError):
        MSKIAMConfig.from_dict(
            {
                "type": "kafka",
                "config": {"bootstrap.servers": "localhost:9092"},
            },
        )


def test_msk_token_provider(mocker: MockerFixture) -> None:
    expiry_time_ms = 1000
    expected_expiry_time_ms = 1.0
    mock_methods = {}
    for method in [
        "generate_auth_token",
        "generate_auth_token_from_profile",
        "generate_auth_token_from_role_arn",
    ]:
        mock_methods[method] = mocker.patch(
            f"aws_msk_iam_sasl_signer.MSKAuthTokenProvider.{method}",
            return_value=("abc:" + method, expiry_time_ms),
        )
    config = MSKIAMConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        region="us-east-1",
    )

    token, expire_time = _oauth_cb(config, None)
    assert token == "abc:generate_auth_token"  # noqa: S105
    assert expire_time == expected_expiry_time_ms
    mock_methods["generate_auth_token"].assert_called_once_with(
        config.region, aws_debug_creds=config.aws_debug_creds
    )

    # Test generate_auth_token_from_profile
    config = MSKIAMConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        region="us-east-1",
        aws_profile="default_profile1",
    )

    token, expire_time = _oauth_cb(config, None)
    assert token == "abc:generate_auth_token_from_profile"  # noqa: S105
    assert expire_time == expected_expiry_time_ms
    mock_methods["generate_auth_token_from_profile"].assert_called_once_with(
        config.region, config.aws_profile
    )

    # Test generate_auth_token_from_role_arn
    config = MSKIAMConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        region="us-east-1",
        role_arn="arn:aws:iam::1234:role/abc",
    )

    token, expire_time = _oauth_cb(config, None)
    assert token == "abc:generate_auth_token_from_role_arn"  # noqa: S105
    assert expire_time == expected_expiry_time_ms
    mock_methods["generate_auth_token_from_role_arn"].assert_called_once_with(config.region, config.role_arn)

    # Test default region
    mocker.patch(
        "openlineage.client.transport.msk_iam._detect_running_region",
        return_value="eu-central-1",
    )
    config = MSKIAMConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )
    MSKIAMTransport(config)
    transport = MSKIAMTransport(config)
    assert transport.msk_config.region == "eu-central-1"

    # Test no region
    mocker.patch(
        "openlineage.client.transport.msk_iam._detect_running_region",
        return_value=None,
    )
    config = MSKIAMConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
    )
    with pytest.raises(
        ValueError,
        match="OpenLineage MSK IAM Transport must have a region defined. "
        "Please use the `region` configuration key to set it.",
    ):
        MSKIAMTransport(config)


def test_setup_producer_configuration(
    mocker: MockerFixture,
) -> None:
    mocker.patch(
        "openlineage.client.transport.kafka._check_if_airflow_sqlalchemy_context",
        return_value=False,
    )
    config = MSKIAMConfig(
        config={"bootstrap.servers": "localhost:9092"},
        topic="random-topic",
        region="us-east-1",
    )

    setup_producer_mocker = mocker.patch(
        "openlineage.client.transport.kafka.KafkaTransport._setup_producer",
    )
    msk_token_mocker = mocker.patch(
        "openlineage.client.transport.msk_iam._oauth_cb", return_value=("token", 1000)
    )
    MSKIAMTransport(config)

    expected_kafka_config = {
        "bootstrap.servers": "localhost:9092",
        "sasl.mechanism": "OAUTHBEARER",
        "security.protocol": "SASL_SSL",
    }
    total_producer_configuration_keys = 4
    args, kwargs = setup_producer_mocker.call_args
    actual_kafka_config = args[0]
    assert len(actual_kafka_config) == total_producer_configuration_keys
    assert "oauth_cb" in actual_kafka_config
    actual_oauth_cb = actual_kafka_config["oauth_cb"]
    del actual_kafka_config["oauth_cb"]
    assert actual_kafka_config == expected_kafka_config
    assert actual_oauth_cb(msk_token_mocker) == ("token", 1000)
