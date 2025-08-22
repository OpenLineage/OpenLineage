---
sidebar_position: 7
---

# Lifecycle State Change Facet

Example:

```json
{
    ...
    "outputs": {
        "facets": {
            "lifecycleStateChange": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
                "lifecycleStateChange": "CREATE"
            }
        }
    }
    ...
}
```

```json
{
    ...
    "outputs": {
        "facets": {
            "lifecycleStateChange": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
                "lifecycleStateChange": "RENAME",
                "previousIdentifier": {
                    "namespace": "example_namespace",
                    "name": "example_table_1"
                }
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json).