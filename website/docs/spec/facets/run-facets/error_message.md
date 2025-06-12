---
sidebar_position: 2
---


# Error Message Facet

The facet to contain information about the failures during the run of the job. A typical payload would be the message, stack trace, etc.

Example:

```json
{
    ...
    "run": {
        "facets": {
            "errorMessage": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/ErrorMessageRunFacet.json",
                "message": "org.apache.spark.sql.AnalysisException: Table or view not found: wrong_table_name; line 1 pos 14",
                "programmingLanguage": "JAVA",
                "stackTrace": "Exception in thread \"main\" java.lang.RuntimeException: A test exception\nat io.openlineage.SomeClass.method(SomeClass.java:13)\nat io.openlineage.SomeClass.anotherMethod(SomeClass.java:9)"
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json)