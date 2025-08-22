---
sidebar_position: 6
---


# SQL Job Facet

The SQL Job Facet contains a SQL query that was used in a particular job.  

Example:

```json
{
    ...
    "job": {
        "facets": {
			"sql": {
				"_producer": "https://some.producer.com/version/1.0",
				"_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/SQLJobFacet.json",
				"query": "select id, name from schema.table where id = 1"
			}
		}
	}
	...
}
```


The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json)