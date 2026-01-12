# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage.common.provider.snowflake import fix_account_name, fix_snowflake_sqlalchemy_uri


@pytest.mark.parametrize(
    "source,target",
    [
        # Account locator format with full region.cloud
        (
            "snowflake://user:pass@xy123456.us-east-1.aws/database/schema",
            "snowflake://xy123456.us-east-1.aws/database/schema",
        ),
        # Account locator without region/cloud - defaults added
        (
            "snowflake://xy123456/database/schema",
            "snowflake://xy123456.us-west-1.aws/database/schema",
        ),
        # Account locator with region only - cloud defaults to aws
        (
            "snowflake://xy12345.ap-southeast-1/database/schema",
            "snowflake://xy12345.ap-southeast-1.aws/database/schema",
        ),
        # Account locator with Azure
        (
            "snowflake://user:pass@xy12345.south-central-us.azure/database/schema",
            "snowflake://xy12345.south-central-us.azure/database/schema",
        ),
        # Account locator with GCP
        (
            "snowflake://user:pass@xy12345.us-east4.gcp/database/schema",
            "snowflake://xy12345.us-east4.gcp/database/schema",
        ),
        # Organization-account format - returned as-is
        (
            "snowflake://user:pass@organization-account/database/schema",
            "snowflake://organization-account/database/schema",
        ),
        # Organization-account with special chars in password
        (
            "snowflake://user:p[ass@organization-account/database/schema",
            "snowflake://organization-account/database/schema",
        ),
        (
            "snowflake://user:pass@organization]-account/database/schema",
            "snowflake://organization%5D-account/database/schema",
        ),
    ],
)
def test_snowflake_sqlite_account_urls(source, target):
    assert fix_snowflake_sqlalchemy_uri(source) == target


# Unit Tests using pytest.mark.parametrize
@pytest.mark.parametrize(
    "name, expected",
    [
        # Account locator format (no hyphen) - add defaults
        ("xy12345", "xy12345.us-west-1.aws"),  # No region/cloud
        ("xy12345.us-west-1", "xy12345.us-west-1.aws"),  # Region only, cloud defaults to aws
        ("xy12345.us-west-1.aws", "xy12345.us-west-1.aws"),  # Already complete locator
        ("xy12345.us-west-2.gcp", "xy12345.us-west-2.gcp"),  # Already complete locator for GCP
        ("abc123.east-us-2.azure", "abc123.east-us-2.azure"),  # Azure complete
        ("xy12345aws", "xy12345aws.us-west-1.aws"),  # Account locator without hyphen
        # Organization-account format (contains hyphen) - return as-is
        ("myorg-account", "myorg-account"),  # Organization-account format
        ("my-organization-1234", "my-organization-1234"),  # Organization-account with multiple hyphens
        ("org123-account456", "org123-account456"),  # Organization-account format
    ],
)
def test_fix_account_name(name, expected):
    assert fix_account_name(name) == expected
    assert (
        fix_snowflake_sqlalchemy_uri(f"snowflake://{name}/database/schema")
        == f"snowflake://{expected}/database/schema"
    )
