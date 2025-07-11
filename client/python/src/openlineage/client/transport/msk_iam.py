# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
import logging
import os
from typing import Any

import attr
from openlineage.client.transport import KafkaConfig, KafkaTransport

log = logging.getLogger(__name__)


def _detect_running_region() -> None | str:
    """Dynamically determine the region."""
    import boto3  # type: ignore[import-not-found]

    checks = [
        # check if set through ENV vars
        os.environ.get("AWS_REGION"),
        # else check if set in config or in boto already
        boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
        boto3.Session().region_name,
    ]
    region: str
    for region in checks:
        if region:
            return region

    return None


@attr.define
class MSKIAMConfig(KafkaConfig):
    # MSK producer config
    # https://github.com/aws/aws-msk-iam-sasl-signer-python

    region: str | None = None
    aws_profile: None | str = None
    role_arn: None | str = None
    aws_debug_creds: bool = False


def _oauth_cb(config: MSKIAMConfig, *_: Any) -> tuple[str, float]:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # type: ignore[import-not-found]

    region = config.region
    if config.aws_profile:
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_profile(
            region, config.aws_profile
        )
    elif config.role_arn:
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_role_arn(
            region, config.role_arn
        )
    # Implement the version to load a custom `botocore.credentials.CredentialProvider` at runtime
    # and calling the method
    # `MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(region, credentials_provider)`
    else:
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
            region, aws_debug_creds=config.aws_debug_creds
        )
    log.debug("Token expiry time: %s region %s", expiry_ms, region)
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch,
    # while the token generator returns expiry in ms
    return auth_token, expiry_ms / 1000


class MSKIAMTransport(KafkaTransport):
    kind = "msk-iam"
    config_class = MSKIAMConfig

    def __init__(self, config: MSKIAMConfig) -> None:
        self.msk_config = config
        super().__init__(config)

    def _setup_producer(self, config: dict) -> None:  # type: ignore[type-arg]
        try:
            log.debug("Use MSK producer with the following config: %s", self.msk_config)

            # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
            if self.msk_config.region is None:
                region = _detect_running_region()
                if region:
                    self.msk_config.region = region
                else:
                    except_message = (
                        "OpenLineage MSK IAM Transport must have a region defined. "
                        "Please use the `region` configuration key to set it."
                    )
                    log.exception(except_message)
                    raise ValueError(except_message)
            config.update(
                {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "OAUTHBEARER",
                    "oauth_cb": functools.partial(_oauth_cb, self.msk_config),
                }
            )
            super()._setup_producer(config)
        except ModuleNotFoundError:
            log.exception(
                "OpenLineage client did not found aws-msk-iam-sasl-signer-python module. "
                "Installing it is required for MSK IAM Transport to work. "
                "You can also get it via `pip install openlineage-python[mskiam]`",
            )
            raise
