---
sidebar_position: 10
---

# Test Run Facet

The facet contains the results of test executions associated with a run, capturing test outcomes and configured severities independently of dataset attribution.

Use this facet to record whether quality checks passed or failed alongside the job run that triggered them. Each `TestExecution` entry captures the test name, its execution outcome (`status`), and optionally the configured severity, expected vs. actual values, and the test body.

Fields per test entry:

| Field | Required | Description |
|---|---|---|
| `name` | yes | Identifier for the test (e.g. `assert_no_orphans`) |
| `status` | yes | Execution outcome: `pass`, `fail`, `skip` |
| `severity` | no | Configured severity: `error` (blocks pipeline) or `warn` (warning only) |
| `type` | no | Test classification, e.g. `not_null`, `unique`, `row_count`, `custom_sql` |
| `description` | no | Human-readable description of what the test checks |
| `expected` | no | Expected value or threshold, serialized as a string |
| `actual` | no | Actual observed value, serialized as a string |
| `content` | no | Test body (e.g. a SQL query) |
| `contentType` | no | Format of `content`, e.g. `sql`, `json`, `expression` |
| `params` | no | Arbitrary key-value pairs for check-specific inputs |

Example:

```json
{
    ...
    "run": {
        "facets": {
            "test": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/TestRunFacet.json",
                "tests": [
                    {
                        "name": "assert_order_ids_unique",
                        "status": "pass",
                        "severity": "error",
                        "type": "unique"
                    },
                    {
                        "name": "assert_row_count_reasonable",
                        "status": "fail",
                        "severity": "warn",
                        "type": "row_count",
                        "expected": "1000",
                        "actual": "999",
                        "content": "SELECT COUNT(*) FROM orders",
                        "contentType": "sql"
                    }
                ]
            }
        }
    }
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/TestRunFacet.json)
