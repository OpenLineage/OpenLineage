---
sidebar_position: 4
---

# Facets & Extensibility

Facets provide context to the OpenLineage events. Generally, an OpenLineage event contains the type of the event, who created it, and when the event happened. In addition to the basic information related to the event, it provides `facets` for more details in four general categories:

- job: What kind of activity ran
- run: How it ran
- inputs: What was used during its run
- outputs: What was the outcome of the run

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
