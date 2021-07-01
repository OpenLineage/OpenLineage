# dbt-snowflake-openlineage

Plugin for [dbt](https://github.com/fishtown-analytics/dbt) (data build tool) for automatic metadata and lineage collection.

This integration is experimental, and there is high possibility that it will change in the future, 
as together with dbt developers we're figuring what's the best way to extract metadata from it.

## Requirements

```
Python 3.6+
sqlparse>=0.3.1
```

## Installation

```
$ pip install marquez-dbt-snowflake
```

## Configuration

You need to configure dbt's [profiles.yaml](https://docs.getdbt.com/dbt-cli/configure-your-profile) file.
To use this plugin you need to use type: openlineage_snowflake.

It handles the same configuration flags as original snowflake plugin, with the addition of OpenLineage related ones:

```bash
    openlineage_url - sets URL of OpenLineage server where events will be pushed
    openlineage_timeout - optional - sets timeout for connecting to OpenLineage server. By default it's set to 5 seconds.
    openlineage_api_key - optional - sets api key for OpenLineage server
```
