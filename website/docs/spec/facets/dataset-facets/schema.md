---
sidebar_position: 6
---

# Schema Dataset Facet

The schema dataset facet contains the schema of a particular dataset. 
Besides a name, it provides an optional type and description of each field.

Nested fields are supported as well.


Example:

```json
{
  ...
  "inputs": {
    "facets": {
      "schema": {
        "_producer": "https://some.producer.com/version/1.0",
        "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
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
          },
          {
            "name": "phones",
            "type": "array",
            "description": "List of phone numbers",
            "fields": [
              {
                "name": "_element",
                "type": "string",
                "description": "Phone number"
              }
            ]
          },
          {
            "name": "address",
            "type": "struct",
            "description": "Customer address",
            "fields": [
              {
                "name": "type",
                "type": "string",
                "description": "Address type, g.e. home, work, etc."
              },
              {
                "name": "country",
                "type": "string",
                "description": "Country name"
              },
              {
                "name": "zip",
                "type": "string",
                "description": "Zip code"
              },
              {
                "name": "state",
                "type": "string",
                "description": "State name"
              },
              {
                "name": "street",
                "type": "string",
                "description": "Street name"
              }
            ]
          },
          {
            "name": "custom_properties",
            "type": "map",
            "fields": [
              {
                "name": "key",
                "type": "string"
              },
              {
                "name": "value",
                "type": "union",
                "fields": [
                  {
                    "name": "_0",
                    "type": "string"
                  },
                  {
                    "name": "_1",
                    "type": "int64"
                  }
                ]
              }
            ]
          }
        ]
      }
    }
  }
  ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json).