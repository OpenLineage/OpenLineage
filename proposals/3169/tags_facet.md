---
Author: Leo Godin 
Created: November tenth, 2024 
Issue: https://github.com/OpenLineage/OpenLineage/issues/3169 
---

**Purpose:**

Arbitrary metadata, often referred to as tagging, concerns adding additional datapoints that are not part of the OL spec to events. 

Several use cases exist for this type of data. 

- Environment information for jobs
- Setting PII/security information for columns
- Grain for datasets
- Technology-specific attributes i.e. (dbt Cloud project, Airflow tags, etc.)
- Downstream notification information like Slack channels, email addresses, etc.
- Configuration information for downstream automation (A message to re-run a pipeline, create Jira ticket, kick off reverse ETL, etc.)
- Enriching events with domain-specific tags
- etc.

No matter how many fields OpenLineage adds to the spec, there will always be a need for additional metadata both supplied by the client and the user. In order to allow OpenLineage to support more use cases, we should consider a generalized and automatable approach to capturing this type of metadata. 

*See these discussions for additional context:*

https://github.com/OpenLineage/OpenLineage/issues/2779
https://github.com/OpenLineage/OpenLineage/issues/2748
https://github.com/OpenLineage/OpenLineage/pull/1630

*Examples*

Arbitrary metadata comes in many forms. Some implementations assign single-string tokens as tags. Others allow key/value pairs or even full dictionaries. Our solution needs to handle all of these cases. Furthermore, the metadata can come from integrations, users or the specific tools we are monitoring with OpenLineage. It would be good to track the provenience of each one. 

Airflow DAG (Single token strings)

```python
    @dag(
        start_date=pendulum.datetime(2024, 6, 10, tz="UTC"),
        tags=["crr_pipeline", "prod", "team=dataos"],
        schedule="5,15,25,35,45,55 * * * *", 
    )
```

dbt meta (Dictionary of arbitrary metadata)

```yaml
   - name: subscription_rate_plan_view
      config:
        meta:
          nr_config:
            team: Order Management 
            alert_failed_test_rows: True 
            failed_test_rows_limit: 20 
            slack_mentions: '@user1, @user2, @user3
```

Snowflake column tags (Schema-level object stored as a key/value pair)

```sql
    ALTER TABLE hr.tables.empl_info
    MODIFY COLUMN job_title
    SET TAG visibility = 'public';
```

User-supplied tag through environment variables

```bash
    export OPENLINEAGE_TAG_ENVIRONMENT=prod
```

Run Event Example with Tags
```json
{
        "eventType": "START",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {
          "runId": "0176a8c2-fe01-7439-87e6-56a1a1b4029f",
          "facets": {
            "tags": [
                {"key": "project", "value": "myproject", "source": "DBT_CLOUD_INTEGRATION"},
                {"key": "environment", "value": "production", "source": "DBT_CLOUD_INTEGRATION"},
                {"key": "crr_pipeline", "value": true, "source": "USER"},
                {"key": "is_adhoc", "value": true, "source": "DBT_CLOUD_INTEGRATION"}
            ]
          }
        },
        "job": {
          "namespace": "my-namespace",
          "name": "my-job",
          "facets": {
              "tags": [
                {"key": "project", "value": "myproject", "source": "DBT_CLOUD_INTEGRATION"},
                {"key": "environment", "value": "production", "source": "DBT_CLOUD_INTEGRATION"},
                {"key": "crr_pipeline", "value": true, "source": "USER"}
              ]
          }
        },
        "inputs": [{
          "namespace": "my-namespace",
          "name": "my-input",
          "facets": {
            "tags": [
                {"key": "meta_nr_config_slack_mentions": "value": "@user1, @user2", "source": "DBT_CLOUD_INTEGRATION"},
                {"key": "pii", "value": true, "field": "email", "source": "DBT_CLOUD_INTEGRATION"}
            ],
            "schema": {
                "fields": [
                    {"name": "email", "type": "string" },
                    {"name": "id", "type": "int"}
                ] 
                
            }
          }
        }],  
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"
      }
```
**Proposed implementation**

This implementation proposes new tags facets that can be applied to jobs, runs, or datasets. The facet will have one property called tags. Tags is an array of objects holding key/value pairs and optional identifying information. While using key/value does not preserve the hierarchical nature of nested objects, it does allow us to collect all needed information supporting the vast majority of use cases.

This schema will include a TagFacet with four properties. 
* key: Identifies the tag by name
* value: The value of the tag. Set to true to mimic single-string token. (pii=true)
* source: (optional) Where the tag came from. This can be from the integration, a user, or the technology we are monitoring. 
* field: (optional) Dataset field the tag applies to

To add context to tags, we will generate TagJobFacet, TagRunFacet and TagDatasetFacet all referencing TagFacet.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/facets/1-1-1/TagFacet.json",
  "$defs": {
    "TagFacetFields": {
      "type": "object",
      "properties": {
        "key": {
          "description": "Key that identifies the tag",
          "type": "string",
          "example": "pii"
        },
        "value": {
          "description": "The value of the field",
          "type": ["string", "number", "boolean"],
          "example": "true|@user1|production"
        },
        "source": {
          "description": "The source of the tag. INTEGRATION|USER|DBT CORE|SPARK|etc.",
          "type": "string"
        },
        "field": {
          "description": "Identifies the field in a dataset if a tag applies to one",
          "type": "string",
          "example": "email_address"
        },
      },
      "required": ["key", "value"]
    },
    "TagFacet": {
      "allOf": [
        {
          "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet"
        },
        {
          "type": "object",
          "properties": {
            "tags": {
              "description": "The tags applied to the facet",
              "type": "array",
              "items": {
                "$ref": "#/$defs/TagFacetFields"
              }
            }
          }
        }
      ],
      "type": "object"
  },
  "TagRunFacet": {
      "allOf": [
        {
          "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/TagFacet"
        },
        {
          "type": "object",
          "properties": {}
          }
      ],
      "type": "object"
  },
  "TagJobFacet": {
      "allOf": [
        {
          "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/TagFacet"
        },
        {
          "type": "object",
          "properties": {}
        }
      ],
      "type": "object"
  },
  "TagDatasetFacet": {
      "allOf": [
        {
          "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/TagFacet"
        },
        {
          "type": "object",
          "properties": {}
        }
      ],
      "type": "object"
  },
}
```

Tag Lifecycle

Tags can be created, modified and deleted. However, it is difficult for stateless integrations to identify changes. The
integrations generally only understand current state. To enable deletion and modification, events should always  
provide the full set of tags. Consumers should consider tags from the latest event to be the complete set of tags. 

Python and Java client

* Implement the tag facet
* Identify user-supplied tags through environment variables i.e. (OPENLINEAGE_TAG__ENVIRONMENT=production)
* Allow integrations to query for user-supplied tags

Integrations

* Identify technology-specific tags and metadata, create the tag facet, attach the tag facet to appropriate parent facets
* Optional - Allow user-supplied tags through configuration or CLI arguments i.e. (--environment=production)

Building Tags

* The combination of key and source should be unique within an event.
* Single-token tags use key=True. i.e. (pii becomes {key: pii, value: true}) 
* Key should match the source data point as closely as possible. i.e. ("team=dataos", "pii" and "production" become key=team, key=pii, key=production)
* User-supplied tags supplied through environment variables or config files should apply to job and run, but not dataset. (Dataset should have a method of supplying tags through an integration)
* In cases where the source metadata is an object, key/value pairs should be built from the object. For example, the dictionary below would be represented as a single tag. 

```yaml
   - name: subscription_rate_plan_view
      config:
        meta:
          nr_config:
            team: Order Management 
```

becomes 

```json
  {
    "key": "meta_nr_config_team",
    "value": "Order Management"
  }
```