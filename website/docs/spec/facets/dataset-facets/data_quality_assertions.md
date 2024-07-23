---
sidebar_position: 3
---

# Data Quality Assertions Facet

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "dataQualityAssertions": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
                "assertions": [
                    {
                        "assertion": "not_null",
                        "success": true,
                        "column": "user_name"
                    },
                    {
                        "assertion": "is_string",
                        "success": true,
                        "column": "user_name"
                    }
                ]
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json).