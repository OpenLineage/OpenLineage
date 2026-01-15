---
sidebar_position: 3
---

# Data Quality Assertions Facet

This facet captures the results of data quality tests performed on a dataset or its columns.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `assertion` | string | Yes | Type of expectation test (e.g., `not_null`, `unique`, `accepted_values`) |
| `success` | boolean | Yes | Whether the assertion passed |
| `column` | string | No | Column being tested. If empty, the assertion refers to the whole dataset |
| `severity` | string | No | Configured severity level: `error` (blocks pipeline) or `warn` (warning only) |
| `failures` | integer | No | The actual number of rows that failed the assertion test |
| `properties` | object | No | Additional tool-specific properties of the assertion |

## Basic Example

```json
{
    "inputs": [{
        "namespace": "postgres://host:5432",
        "name": "public.orders",
        "facets": {
            "dataQualityAssertions": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json",
                "assertions": [
                    {
                        "assertion": "not_null",
                        "success": true,
                        "column": "user_name"
                    },
                    {
                        "assertion": "unique",
                        "success": true,
                        "column": "order_id"
                    }
                ]
            }
        }
    }]
}
```

## dbt Example

When capturing assertions from dbt tests, additional metadata can be included:

```json
{
    "inputs": [{
        "namespace": "postgres://host:5432",
        "name": "public.orders",
        "facets": {
            "dataQualityAssertions": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/dbt",
                "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json",
                "assertions": [
                    {
                        "assertion": "not_null",
                        "success": true,
                        "column": "order_id",
                        "severity": "error",
                        "failures": 0,
                        "properties": {
                            "warnIf": "!= 0",
                            "errorIf": "!= 0",
                            "failCalc": "count(*)"
                        }
                    },
                    {
                        "assertion": "accepted_values",
                        "success": false,
                        "column": "status",
                        "severity": "warn",
                        "failures": 3,
                        "properties": {
                            "warnIf": "!= 0",
                            "errorIf": "> 10",
                            "failCalc": "count(*)"
                        }
                    },
                    {
                        "assertion": "relationships",
                        "success": true,
                        "column": "customer_id",
                        "severity": "error",
                        "failures": 0,
                        "properties": {
                            "warnIf": "!= 0",
                            "errorIf": "!= 0",
                            "failCalc": "count(*)"
                        }
                    }
                ]
            }
        }
    }]
}
```

### dbt-specific Properties

The `properties` object can contain dbt test configuration:

| Property | Description | Example |
|----------|-------------|---------|
| `warnIf` | Condition expression that triggers a warning | `!= 0`, `> 5` |
| `errorIf` | Condition expression that triggers an error | `!= 0`, `> 10` |
| `failCalc` | SQL expression used to calculate the failure value | `count(*)` |

These correspond to dbt's [test severity configuration](https://docs.getdbt.com/reference/resource-configs/severity).

## Specification

The facet specification can be found [here](https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json).