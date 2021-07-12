from dbt.adapters.base import AdapterPlugin

from dbt.adapters.openlineage_bigquery.connections import OpenLineageBigQueryCredentials, \
    OpenLineageBigQueryConnectionManager  # noqa
from dbt.adapters.openlineage_bigquery.impl import OpenLineageBigQueryAdapter
from dbt.include import openlineage_bigquery

__author__ = """Marquez Project"""
__version__ = "0.16.1rc1"

# Caution: this plugin is experimental. More information in README.md

Plugin = AdapterPlugin(
    adapter=OpenLineageBigQueryAdapter,
    credentials=OpenLineageBigQueryCredentials,
    include_path=openlineage_bigquery.PACKAGE_PATH,
    dependencies=['bigquery'])
