---
sidebar_position: 4
---

# Facets & Extensibility

A facet is an atomic piece of metadata identified by its name.  
Facets provide context to the OpenLineage events, they are pieces of metadata that can be attached to the core entities of the spec:
- Run
- Job
- Dataset (Inputs or Outputs)

Generally, an OpenLineage event contains the type of the event, who created it, and when the event happened. 
In addition to the basic information related to the event, it provides `facets` for more details in four general categories:

- job: What kind of activity ran
- run: How it ran
- inputs: What was used during its run
- outputs: What was the outcome of the run

## Custom Facet Naming

Emitting a new facet with the same name for the same entity replaces the previous facet instance for that entity entirely.
It is defined as a JSON object that can be either part of the spec or a custom facet defined in a different project.

Custom facets must use a distinct prefix named after the project defining them to avoid collision with standard facets defined in the [OpenLineage.json](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) spec.
They have a `_schemaURL` field pointing to the corresponding version of the facet schema (as a JSONPointer: [$ref URL location](https://swagger.io/docs/specification/using-ref/) ).

For example: https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/MyCustomJobFacet

The versioned URL must be an immutable pointer to the version of the facet schema. For example, it should include a tag of a git sha and not a branch name. 
This should also be a canonical URL. There should be only one URL used for a given version of a schema.

Custom facets can be promoted to the standard by including them in the spec.

The naming of custom facets should follow the pattern `{prefix}{name}{entity}Facet` PascalCased.  
The prefix must be a distinct identifier named after the project defining it to avoid collision with standard facets defined in the [OpenLineage.json](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) spec.
The entity is the core entity for which the facet is attached.

When attached to the core entity, the key should follow the pattern `{prefix}_{name}`, where both prefix and name follow snakeCase pattern.
An example of a valid name is `BigQueryStatisticsJobFacet` and its key `bigQuery_statistics`.


## Example

Here is an example of the four facets in action. Notice the element `facets` under each of the four categories of the OpenLineage event:

```json
{
  "eventType": "START",
  "eventTime": "2020-12-28T19:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
    "facets": {
        "parent": {
            "job": {
                "name": "dbt-execution-parent-job", 
                "namespace": "dbt-namespace"
            },
            "run": {
                "runId": "f99310b4-3c3c-1a1a-2b2b-c1b95c24ff11"
            }
        }
    }
  },
  "job": {
    "namespace": "workshop",
    "name": "process_taxes",
    "facets": {
        "sql": {
            "query": "insert into taxes_out select id, name, is_active from taxes_in"
        }
    }
  },
  "inputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.taxes-in",
    "facets": {
        "schema": {
            "fields": [
                {
                "name": "id",
                "type": "int",
                "description": "Customer's identifier"
                },
                {
                "name": "name",
                "type": "string",
                "description": "Customer's name"
                },
                {
                "name": "is_active",
                "type": "boolean",
                "description": "Has customer completed activation process"
                }
            ]
        }
    }
  }],  
  "outputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.taxes-out",
    "facets": {
        "schema": {
            "fields": [
                {
                "name": "id",
                "type": "int",
                "description": "Customer's identifier"
                },
                {
                "name": "name",
                "type": "string",
                "description": "Customer's name"
                },
                {
                "name": "is_active",
                "type": "boolean",
                "description": "Has customer completed activation process"
                }
            ]
        }
    }
  }],  
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```
For more information of what kind of facets are available as part of OpenLineage spec, please refer to the sub sections `Run Facets`, `Job Facets`, and `Dataset Facets` of this document.
