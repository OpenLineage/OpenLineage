---
sidebar_position: 1
---

# Job Documentation Facet

Contains the documentation or description of the job.

Example:

```json
{
    ...
    "job": {
        "facets": {
            "documentation": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/DocumentationJobFacet.json",
                "description": "This is the documentation of something.",
                "contentType": "text/markdown"
            }
        }
    }
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json)
