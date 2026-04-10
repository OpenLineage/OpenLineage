---
sidebar_position: 3
---

# Data Quality Assertions Facet

The facet records the results of tests (e.g. data quality assertions) run against a specific dataset or its columns. Each entry captures what was asserted, whether it passed, which column it targets, and optionally rich metadata such as the observed vs. expected values and the assertion body.

Use this facet to attach test outcomes directly to the dataset they validate. For run-level test reporting that is independent of any particular dataset, see the [Test Run Facet](../run-facets/test_run.md).

Fields per assertion entry:

| Field | Required | Description |
|---|---|---|
| `assertion` | yes | Test classification, e.g. `not_null`, `unique`, `row_count`, `custom_sql`. Equivalent to `type` in `TestRunFacet`. |
| `success` | yes | Whether the assertion passed (`true`) or failed (`false`). Equivalent to `status: "pass"/"fail"` in `TestRunFacet`. See `success` vs `severity` paragraph below. |
| `column` | no | Column being tested; omit for table-level assertions |
| `name` | no | Identifier for the test (e.g. `assert_no_orphans`). |
| `severity` | no | Configured severity: `error` (failure blocks pipeline) or `warn` (warning only). See `success` vs `severity` paragraph below. |
| `description` | no | Human-readable description of what the assertion checks |
| `expected` | no | Expected value or threshold, serialized as a string |
| `actual` | no | Actual observed value, serialized as a string |
| `content` | no | Assertion body (e.g. a SQL query) |
| `contentType` | no | Format of `content`, e.g. `sql`, `json`, `expression` |
| `params` | no | Arbitrary key-value pairs for assertion-specific inputs |

## `success` vs `severity`

These two fields are independent and serve different purposes:

- **`success`** reflects whether the test *found issues*: `true` means no issues were found, `false` means issues were found (regardless of whether the final pipeline execution was blocked).
- **`severity`** reflects the *configured consequence* of a failure: `error` means a failure blocks pipeline execution, `warn` means a failure produces a warning only and does not block.

A test can have `success: false` and `severity: "warn"` — meaning the test detected a violation, but execution continued. The overall run still succeeds. This combination is important to preserve in lineage metadata because it lets consumers distinguish between tests that are enforced hard constraints and tests that are advisory checks.

## Relationship to TestRunFacet

`DataQualityAssertionsDatasetFacet` and `TestRunFacet` serve similar purposes with few differences:

| | `DataQualityAssertionsDatasetFacet` | `TestRunFacet` |
|---|---|---|
| **Attached to** | A specific dataset | The run as a whole |
| **Test type field** | `assertion` | `type` |
| **Result field** | `success` (boolean) | `status` (string: `pass`/`fail`/`skip`) |
| **Column scope** | `column` field (dataset- or column-level) | Not applicable |
| **Use when** | Asserting quality of a particular dataset/column | Recording test executions for a run, independently of datasets |

## Example

```json
{
    ...
    "inputs": [{
        "facets": {
            "dataQualityAssertions": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-3/DataQualityAssertionsDatasetFacet.json",
                "assertions": [
                    {
                        "assertion": "not_null",
                        "success": true,
                        "column": "order_id",
                        "severity": "error"
                    },
                    {
                        "assertion": "row_count",
                        "name": "assert_order_row_count",
                        "success": false,
                        "severity": "warn",
                        "description": "Expected at least 1000 rows",
                        "expected": "1000",
                        "actual": "999",
                        "content": "SELECT COUNT(*) FROM orders",
                        "contentType": "sql",
                        "params": {"threshold": "0.01"}
                    }
                ]
            }
        }
    }]
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json).
