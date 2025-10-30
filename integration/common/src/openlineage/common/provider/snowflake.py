# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import quote, urlparse, urlunparse


def fix_account_name(name: str) -> str:
    """
    Normalize Snowflake account name according to OpenLineage specification.

    Snowflake supports two formats:
    1. Organization-account format (preferred): orgname-accountname
       - Never includes region or cloud in the identifier
       - Example: myorg-myaccount

    2. Account locator format (legacy): accountlocator[.region[.cloud]]
       - Examples:
         - xy12345 → xy12345.us-west-1.aws (AWS US West Oregon default)
         - xy12345.us-east-1 → xy12345.us-east-1.aws (aws default)
         - xy12345.us-east-2.aws → xy12345.us-east-2.aws
         - xy12345.east-us-2.azure → xy12345.east-us-2.azure
    """
    # Split by dots to analyze the structure
    parts = name.split(".")

    # First part is the account identifier
    account_part = parts[0]

    # Check if it's organization-account format (contains hyphen)
    # Organization-account format never has region/cloud info
    if "-" in account_part:
        # Organization-account format - return as-is
        return account_part

    # Account locator format - need to include region and cloud
    if len(parts) == 1:
        # Just account locator, add default region and cloud (AWS US West Oregon)
        return f"{account_part}.us-west-1.aws"
    elif len(parts) == 2:
        # account_locator.region, add default cloud (aws)
        return f"{account_part}.{parts[1]}.aws"
    else:
        # Full format: account_locator.region.cloud
        return name


def fix_snowflake_sqlalchemy_uri(uri: str) -> str:
    """Snowflake sqlalchemy connection URI has following structure:
    'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
    We want to canonicalize account identifier. It can have two forms:
    - newer, in form of <organization>-<id>. In this case we want to do nothing.
    - older, composed of <id>-<region>-<cloud> where region and cloud can be
      optional in some cases.If <cloud> is omitted, it's AWS.
      If region and cloud are omitted, it's AWS us-west-1
    """

    try:
        parts = urlparse(uri)
    except ValueError:
        # snowflake.sqlalchemy.URL does not quote `[` and `]`
        # that's a rare case so we can run more debugging code here
        # to make sure we replace only password
        parts = urlparse(uri.replace("[", quote("[")).replace("]", quote("]")))

    hostname = parts.hostname
    if not hostname:
        return uri

    hostname = fix_account_name(hostname)
    # else - its new hostname, just return it
    return urlunparse((parts.scheme, hostname, parts.path, parts.params, parts.query, parts.fragment))
