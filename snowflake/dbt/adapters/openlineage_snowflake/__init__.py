from dbt.adapters.base import AdapterPlugin

from dbt.adapters.openlineage_snowflake.connections import OpenLineageSnowflakeCredentials, \
    OpenLineageSnowflakeConnectionManager  # noqa
from dbt.adapters.openlineage_snowflake.impl import OpenLineageSnowflakeAdapter
from dbt.include import openlineage_snowflake

__author__ = """Marquez Project"""
__version__ = "0.16.1rc1"

# Caution: this plugin is experimental. More information in README.md

Plugin = AdapterPlugin(
    adapter=OpenLineageSnowflakeAdapter,
    credentials=OpenLineageSnowflakeCredentials,
    include_path=openlineage_snowflake.PACKAGE_PATH,
    dependencies=['snowflake'])
