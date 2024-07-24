---
title: Example Lineage Events
sidebar_position: 2
---

## Simple Examples

### START event with single input

This is a START event with a single PostgreSQL input dataset.

```json
{
  "eventType": "START",
  "eventTime": "2020-12-28T19:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
  },
  "job": {
    "namespace": "workshop",
    "name": "process_taxes"
  },
  "inputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.taxes"
  }],  
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

### COMPLETE event with single output

This is a COMPLETE event with a single PostgreSQL output dataset.

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2020-12-28T20:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
  },
  "job": {
    "namespace": "workshop",
    "name": "process_taxes"
  },
  "outputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.unpaid_taxes"
  }],
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

## Complex Examples

### START event with Facets (run and job)

This is a START event with run and job facets of Apache Airflow.

```json
{
  "eventType": "START",
  "eventTime": "2020-12-28T19:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
    "facets": {
      "airflow_runArgs": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/BaseFacet",
        "externalTrigger": true
      },
      "nominalTime": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/NominalTimeRunFacet",
        "nominalStartTime": "2022-07-29T14:14:31.458067Z"
      },
      "parentRun": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/ParentRunFacet",
        "job": {
          "name": "etl_orders",
          "namespace": "cosmic_energy"
        },
        "run": {
          "runId": "1ba6fdaa-fb80-36ce-9c5b-295f544ec462"
        }
      }
    }
  },
  "job": {
    "namespace": "workshop",
    "name": "process_taxes",
    "facets": {
      "documentation": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/DocumentationJobFacet",
        "description": "Process taxes."
      },
      "sql": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
        "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SqlJobFacet",
        "query": "INSERT into taxes values(1, 100, 1000, 4000);"
      }
    },
  },
  "inputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.taxes"
  }],  
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

### COMPLETE event with Facets (dataset)

This is a COMPLETE event with dataset facet of Database table.

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2020-12-28T20:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
  },
  "job": {
    "namespace": "workshop",
    "name": "process_taxes"
  },
  "outputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.unpaid_taxes",
    "facets": {
        "dataSource": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
          "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/DataSourceDatasetFacet",
          "name": "postgres://workshop-db:None",
          "uri": "workshop-db"
        },
        "schema": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/tree/0.10.0/integration/airflow",
          "_schemaURL": "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
          "fields": [
            {
              "name": "id",
              "type": "SERIAL PRIMARY KEY"
            },
            {
              "name": "tax_dt",
              "type": "TIMESTAMP NOT NULL"
            },
            {
              "name": "tax_item_id",
              "type": "INTEGER REFERENCES tax_itemsid"
            },
            {
              "name": "amount",
              "type": "INTEGER NOT NULL"
            },
            {
              "name": "ref_id",
              "type": "INTEGER REFERENCES refid"
            },
            {
              "name": "comment",
              "type": "TEXT"
            }
          ]
        }
      }
  }],     
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```