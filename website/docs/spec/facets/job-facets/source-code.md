---
sidebar_position: 3
---

# Source Code Facet

The source code of a particular job (e.g. Python script)

Example:

```json
{
    ...
    "job": {
        "facets": {
            "sourceCode": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SourceCodeJobFacet.json",
                "language": "python",
                "sourceCode": "print('hello, OpenLineage!')"
            }
        }
    }
    ...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/SourceCodeJobFacet.json)