# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from urllib.parse import urlparse, urlunparse


def fix_account_name(name: str) -> str:
    spl = name.split('.')
    if len(spl) == 1:
        account = spl[0]
        region, cloud = 'us-west-1', 'aws'
    elif len(spl) == 2:
        account, region = spl
        cloud = 'aws'
    else:
        account, region, cloud = spl
    return f"{account}.{region}.{cloud}"


def fix_snowflake_sqlalchemy_uri(uri: str) -> str:
    """Snowflake sqlalchemy connection URI has following structure:
        'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
        We want to canonicalize account identifier. It can have two forms:
        - newer, in form of <organization>-<id>. In this case we want to do nothing.
        - older, composed of <id>-<region>-<cloud> where region and cloud can be
          optional in some cases.If <cloud> is omitted, it's AWS.
          If region and cloud are omitted, it's AWS us-west-1
    """

    parts = urlparse(uri)

    hostname = parts.hostname
    if not hostname:
        return uri

    # old account identifier like xy123456
    if '.' in hostname or not any(word in hostname for word in ['-', '_']):
        hostname = fix_account_name(hostname)
    # else - its new hostname, just return it
    return urlunparse(
        (parts.scheme, hostname, parts.path, parts.params, parts.query, parts.fragment)
    )
