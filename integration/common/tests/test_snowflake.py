# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import pytest
from openlineage.common.provider.snowflake import fix_snowflake_sqlalchemy_uri


@pytest.mark.parametrize("source,target", [
    ("snowflake://user:pass@xy123456.us-east-1.aws/database/schema",
        "snowflake://xy123456.us-east-1.aws/database/schema"),
    ("snowflake://xy123456/database/schema", "snowflake://xy123456.us-west-1.aws/database/schema"),
    ("snowflake://xy12345.ap-southeast-1/database/schema",
        "snowflake://xy12345.ap-southeast-1.aws/database/schema"),
    ("snowflake://user:pass@xy12345.south-central-us.azure/database/schema",
        "snowflake://xy12345.south-central-us.azure/database/schema"),
    ("snowflake://user:pass@xy12345.us-east4.gcp/database/schema",
        "snowflake://xy12345.us-east4.gcp/database/schema"),
    ("snowflake://user:pass@organization-account/database/schema",
        "snowflake://organization-account/database/schema"),
    ("snowflake://user:p[ass@organization-account/database/schema",
        "snowflake://organization-account/database/schema"),
    ("snowflake://user:pass@organization]-account/database/schema",
        "snowflake://organization%5D-account/database/schema")
])
def test_snowflake_sqlite_account_urls(source, target):
    assert fix_snowflake_sqlalchemy_uri(source) == target
