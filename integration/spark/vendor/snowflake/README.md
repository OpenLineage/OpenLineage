# Snowflake Spark Vendor

This project implements logic to extract an OpenLineage.Dataset for addition to the input and output datasets list 
when Snowflake is used as a source in Spark for read or write operations.

## How
Extract options provided by the Snowflake connector from `SnowflakeRelation` or `snowflake.DefaultSource`.

## Limitations
If the table name returns `COMPELX`, it indicates that the Spark job uses the parameter `query` instead of `dbtable`.

## References
[Snowflake Spark Connector user guide](https://docs.snowflake.com/en/user-guide/spark-connector-use)