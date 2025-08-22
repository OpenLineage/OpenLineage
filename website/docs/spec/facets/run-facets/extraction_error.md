---
sidebar_position: 4
---


# Extraction Error Facet

The facet reflects internal processing errors of OpenLineage. For example, it allows to distinguish SQL job that was parsed and found no datasets processed, from the one which cannot be parsed.

Fields:
- `totalTasks`: The number of distinguishable tasks in a run that were processed by OpenLineage, whether successfully or not. Those could be, for example, distinct SQL statements.
- `failedTasks`: The number of distinguishable tasks in a run that were processed not successfully by OpenLineage. Those could be, for example, distinct SQL statements.
- `errors`: Array of error objects:
  - `taskNumber`: Order of task (counted from 0).
  - `task`: Text representation of task that failed. This can be, for example, SQL statement that parser could not interpret.
  - `errorMessage`: Text representation of extraction error message.
  - `stackTrace`: Stack trace of extraction error message

Example:

```json
{
    ...
    "run": {
        "facets": {
            "extractionError": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ExtractionErrorRunFacet.json",
                "totalTasks": "2",
                "failedTasks": "1",
                "errors": [
                    {
                        "taskNumber": 0,
                        "task": "DROP POLICY IF EXISTS name ON table_name",
                        "errorMessage": "Expected TABLE, VIEW, INDEX, ROLE, SCHEMA, FUNCTION, STAGE or SEQUENCE after DROP, found: POLICY at Line: 1, Column 6",
                        "stackTrace": null
                    },
                ]
            }
        }
    }
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/ExtractionErrorRunFacet.json)
