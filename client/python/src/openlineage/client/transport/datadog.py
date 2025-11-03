# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import attr
from openlineage.client.transport.async_http import AsyncHttpConfig, AsyncHttpTransport
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.transport.transport import Config, Transport
from openlineage.client.utils import get_only_specified_fields

if TYPE_CHECKING:
    from openlineage.client.client import Event


# A mapping from https://docs.datadoghq.com/getting_started/site/ to
# Datadog endpoint that accepts OL events.
SITE_MAPPING = {
    "datadoghq.com": "https://data-obs-intake.datadoghq.com",
    "us3.datadoghq.com": "https://data-obs-intake.us3.datadoghq.com",
    "us5.datadoghq.com": "https://data-obs-intake.us5.datadoghq.com",
    "datadoghq.eu": "https://data-obs-intake.datadoghq.eu",
    "ddog-gov.com": "https://data-obs-intake.ddog-gov.com",
    "ap1.datadoghq.com": "https://data-obs-intake.ap1.datadoghq.com",
    "ap2.datadoghq.com": "https://data-obs-intake.ap2.datadoghq.com",
    "datad0g.com": "https://data-obs-intake.datad0g.com",
}


def _is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


@attr.define
class DatadogConfig(Config):
    apiKey: str = attr.field()  # noqa: N815
    site: str = attr.field(default="datadoghq.com")
    timeout: float = attr.field(default=5.0)
    retry: dict[str, Any] = attr.field(
        default={
            "total": 5,
            "read": 5,
            "connect": 5,
            "backoff_factor": 0.3,
            "status_forcelist": [500, 502, 503, 504],
            "allowed_methods": ["HEAD", "POST"],
        }
    )
    max_queue_size: int = attr.field(default=10000)
    max_concurrent_requests: int = attr.field(default=100)
    async_transport_rules: dict[str, dict[str, bool]] = attr.field(default={"dbt": {"*": True}})

    @classmethod
    def from_dict(cls, params: dict[str, Any]) -> DatadogConfig:
        api_key = params.get("apiKey") or params.get("apikey") or os.getenv("DD_API_KEY")
        if not api_key:
            msg = (
                "apiKey is required for DatadogTransport. "
                "Provide it in config or set DD_API_KEY environment variable."
            )
            raise ValueError(msg)

        site = params.get("site") or os.getenv("DD_SITE", "datadoghq.com")

        if site not in SITE_MAPPING and not _is_valid_url(site):  # type: ignore
            msg = f"Invalid site '{site}'. Must be one of: {list(SITE_MAPPING.keys())} or a valid URL"
            raise ValueError(msg)

        config_params = params.copy()
        config_params["apiKey"] = api_key
        config_params["site"] = site

        specified_dict = get_only_specified_fields(cls, config_params)
        return cls(**specified_dict)


class DatadogTransport(Transport):
    kind = "datadog"
    config_class = DatadogConfig

    def __init__(self, config: DatadogConfig) -> None:
        self.config = config

        if _is_valid_url(config.site):
            intake_url = config.site
        else:
            intake_url = SITE_MAPPING[config.site]

        shared_config = {
            "url": intake_url,
            "timeout": config.timeout,
            "retry": config.retry,
            "compression": "gzip",
            "auth": {"type": "api_key", "apiKey": config.apiKey},
        }

        http_config = HttpConfig.from_dict(shared_config)
        self.http = HttpTransport(http_config)

        async_config = shared_config.copy()
        async_config.update(
            {
                "max_queue_size": config.max_queue_size,
                "max_concurrent_requests": config.max_concurrent_requests,
            }
        )
        async_http_config = AsyncHttpConfig.from_dict(async_config)
        self.async_http = AsyncHttpTransport(async_http_config)

    def emit(self, event: Event) -> Any:
        # Check if event has JobTypeJobFacet with integration = "dbt"
        should_use_async = self._should_use_async_transport(event)

        if should_use_async:
            return self.async_http.emit(event)
        else:
            return self.http.emit(event)

    def close(self, timeout: float = -1) -> bool:
        http_result = self.http.close(timeout)
        async_result = self.async_http.close(timeout)
        return http_result and async_result

    def _should_use_async_transport(self, event: Event) -> bool:
        """
        Determine if event should use async transport based on async_transport_rules.
        Returns True if the event matches the configured rules for async transport.
        """

        # Check for double wildcard first - if present, use async for all events
        wildcard_rules = self.config.async_transport_rules.get("*", {})
        if wildcard_rules.get("*"):
            return True

        if not hasattr(event, "job") or not hasattr(event.job, "facets"):
            return False

        job_facets = event.job.facets
        if not job_facets:
            return False

        job_type_facet = job_facets.get("jobType")
        if not job_type_facet:
            return False

        integration = getattr(job_type_facet, "integration", "").lower()
        job_type = getattr(job_type_facet, "jobType", "").lower()

        if not integration:
            return False

        for rule_integration, job_type_rules in self.config.async_transport_rules.items():
            if rule_integration == "*" or rule_integration.lower() == integration:
                for rule_job_type, use_async in job_type_rules.items():
                    if rule_job_type == "*" or rule_job_type.lower() == job_type:
                        return use_async
        return False
