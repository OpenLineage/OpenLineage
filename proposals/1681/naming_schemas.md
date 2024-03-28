---
Author: Benji Lampel
Created: 03/07/2023
Issue: https://github.com/OpenLineage/OpenLineage/issues/1681
---

**Purpose**
The Naming.md file should be reworked as a more programmatic solution with clear, specific definitions.

Names and Namespaces are currently slightly nebulous concepts. They can best be described, perhaps, by two components that, together, form a unique URI to a specific dataset for datasets, and a unique name for a Job. This is a serviceable definition for databases, datalakes, and distributed file stores, where a well-defined path exists by nature of the structure. But this may not be the case with database-like systems, or non-database systems, like Salesforce or Google Sheets, respectively. These instances need another way to specify a unique name, one that may not resolve to a URI.

Further, the[Naming.md](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md) has some shortcomings besides support for only databases, datalakes, and distributed file stores:

- Little information, beyond Airflow and Spark, on how Jobs are named within integrations
- Limited set of dataset names, no specific process for adding more
- No definitions of any major terms
- Not programmatic; reference only

There also seems to be inconsistency with naming between certain integrations (at the time of this writing, the SnowflakeExtractor and Great Expectations emit different namespaces and names for Snowflake datasets. This proposal would help rectify issues that cause these discrepancies and ensure that when changes to naming happen, they are consistent across all integrations. The easiest way to do this may be to make things more programmatic, or at least have the ability to test naming changes against integrations.

**Proposed implementation**
The Naming.md file should be transitioned to a set of JSON schema files that specify the naming conditions for a particular integration or source, which can then be used to build a reference file.

The file hierarchy should look like:
```
spec/
  CONTRIBUTING.md
  naming/
    jobs/
      airflow/
        airflow.json
      ...
    datasets/
      snowflake/
        snowflake.json
      ...
```

Where CONTRIBUTING.md (or something like it) would take the place of the current Naming.md for purposes of explaining naming philosophy and how to add or update files in the new file structure, and Naming.md would transition to be a reference file built from the JSON files under `naming/`. CONTRIBUTING.md would also provide clear definitions for all major terms, including: name, namespace, datasource hierarchy, naming hierarchy, scheme, authority, 

Each file under `naming/` would specify the particular convention for that integration or dataset. However, they would have some common required fields. The base schema would look something like:

[Base Example]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/1-0-0/Naming.json",
  "$defs": {
    # Specifies how names and namespaces are combined, as well as case sensitivity
    "DatasetUniqueName": {
        "unique_name_qualifier": {
          "description": "How the namespace and name are combined to form a universally unique name.",
          "type": "string",
          "example": "/"
        },
        "case_sensitivity": {
          "description": "Whether or not the integration uses case-sensitive names.",
          "type": "object",
          "properties": {
            "case_sensitive": {
              "type": "boolean",
            },
            "case": {
              "type": ["string", "null"],
              "enum": ["upper", "lower", null]
            }
          },
          "required": ["unique_name_qualifier", "case_sensitive"]
        }
    },
    "DatasetNamespace": {
      "type": "object",
      "properties": {
        "namespace": {
          "description": "The components that describe the namespace for this integration.",
          "type": "object",
          "properties": {
            "scheme": {
              "description": "The base of a uri that specifies where the source object came from.",
              "type": "string",
              "example": "uri:"
            },
            "userinfo": {
              "description": "User info necessary for authentication.",
              "type": "string",
              "example": "account_string_123"
            },
            # other common namespace properties could go here to be ref'd by implementations
            "host": {
              ...
            },
            "port": {
              ...
            },
            "path": {
              ...
            },
            "query": {
              ...
            },
            "fragment": {
              ...
            }
          },
          "example": "uri:path",
          "required": ["scheme", "path"] # require only these in accordance with standard URI syntax
        }
      }
    },
    "DatasetName": {
      "type": "object",
      "properties": {
        "name": {
          "description": "The name of the dataset, unique within a namespace.",
          "type": "object",
          "properties": {
            # like namespace, outline common properties to ref, but don't need to make any required to keep flexbility
            "database": {
              {"type": "string"}
            },
            "schema": {
              ...
            },
            "table": {
              ...
            }
          },
          "example": "db.schema.table"
        },
      }
    },
    "JobNameBase": {
      ...
    }
  },
  "$ref": "#/$defs/DatasetUniqueName"
}
```

In the above example, the Naming.json schema is provided with a detailed example for Dataset naming. The schema would live in a new `spec/naming` folder under the name `Naming.json`. This would be analogous to the `spec/OpenLineage.json` file that outlines the `RunEvent` and other top-level OpenLineage abstractions. An example is given only for Dataset naming for brevity. The goal of this example is to show how properties for a `namespace` and `name` can be defined generically with a JSON schema, to be implemented by individual integrations. For a Dataset, four pieces of information are needed:
  1. The `namespace`, which minimally should include a `scheme` and a `path` as this seems to be consistent with URI naming conventions, see the [syntax diagram](https://upload.wikimedia.org/wikipedia/commons/d/d6/URI_syntax_diagram.svg). The base spec can also define any URI attributes, such as `auth`/`userinfo`, `host`, or `port`, to be `$ref`'d by implementers.
  2. The `name`, whose required properties, if any, are left to discussion. Again, properties can be defined to be re-used in implementations.
  3. The `unique_name_qualifier`, which is a character or short string that will combine the `namespace` and `name`. Often, this will simply be a `/`.
  4. The `case_sensitivity` is an object that will allow case-sensitive enforcement. This is necessary for some implementers, like Snowflake, and will ensure that all integrations correctly capitalize datasets.

Each of these is specified as a `$def`, or part of a `$def`, in the base schema to provide the template that implementations must conform to. These could be referenced by the [Dataset object in the spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json#L202), which would allow for easy validation of any Dataset.

A similar set of requirements can be developed for Jobs. In this example, each potential element of a `namespace` or `name` is given explicitly as an object. This has the benefit of creating re-usable properties, even if it is verbose. Additionally, some of these properties could be defined as `pattern`s instead of `string`s to enforce certain conventions.

Below is an example implementation for Dataset naming with Snowflake, which will be defined with respect to the given spec.

[Snowflake Example]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/snowflake/1-0-0/Snowflake.json",
  "$defs": {
    "SnowflakeDataset": {
      "allOf": [{
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetUniqueName",
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetNamespace",
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetName/database"
      }],
    }
  },
  "type": "object",
  "properties": {
    "name": {
      "$ref": "#/$defs/SnowflakeDataset"
    }
  }
}
```

In the Snowflake example above, the `SnowflakeDataset` would be implemented simply by referencing the `DatasetUniqueName`, `DatasetNamespace`, and `DatasetName` schemas. This simplifies the implementation greatly, but may cause some other issues. For one, there are no specific `DatasetName` elements that the Snowflake integration specifies -- as long as it satisfies the `DatasetName` spec generally, it's considered valid, although a name in Snowflake is currently defined as: database, schema, and table.

We could modify the base schema above to use a `DatasetNamespace` and `DatasetUniqueName` in a similar way, but have the elements of the `DatasetName` enumerated such that the implementer can pick and choose specific ones.

[Dataset Example 2]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/1-0-0/Naming.json",
  "$defs": {
    "DatasetUniqueName": {
        "name_pattern": {
          "description": "How the name elements are combined to form a name for a Dataset.",
          "type": "pattern",
          "example": "^(?<database>[A-Z]+)\.(?<schema>[A-Z]+)\.(?<table>[A-Z]+)$"
        },
        "unique_name_qualifier": {
          "description": "How the namespace and name are combined to form a universally unique name.",
          "type": "string",
          "example": "/"
        },
        "case_sensitivity": {
          "description": "Whether or not the integration uses case-sensitive names.",
          "type": "object",
          "properties": {
            "case_sensitive": {
              "type": "boolean",
            },
            "case": {
              "type": ["string", "null"],
              "enum": ["upper", "lower", null]
            }
          },
          "required": ["unique_name_qualifier", "case_sensitive"]
        }
    },
    "DatasetNamespace": {
      ... # same object as above
    },
    # All the name potential naming elements
    "Database": {"type": "string"},
    "Schema": {"type": "string"},
    "Table": {"type": "string"},
    "File": {"type": "string"},
    "ProjectId": {"type": "string"}, # BigQuery
    "DatasetName": {"type": "string"}, # BigQuery
    # Job properties below
    "JobNamespace": {...}
  }
  "$ref": "#/$defs/DatasetUniqueName"
}
```

[Snowflake Example 2]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/snowflake/1-0-0/Snowflake.json",
  "$defs": {
    "SnowflakeDataset": {
      "allOf": [{
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetUniqueName",
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetNamespace",
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/Database",
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/Schema",
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/Table"
      }],
    }
  },
  "type": "object",
  "properties": {
    "name": {
      "$ref": "#/$defs/SnowflakeDataset"
    }
  }
}
```

In the above two examples, the `SnowflakeDataset` now implements on certain naming elements from the base spec, making the allowed values explicit. A pattern is added to the `DatasetUniqueName` that will allow the implementer to specify exactly how the elements of the `name` should be combined, similar to how it defines how the `name` and `namespace` should be combined.

Ultimately, these JSON schemas should get created into classes with various attributes and validation.

In addition to the documents themselves, some testing framework should be developed (or tests simply added to integrations) to ensure that naming matches the structure and casing of the JSON in the files exactly. This should include a validation framework available in the client that integrations can run against.
