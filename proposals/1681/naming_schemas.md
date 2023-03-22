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
    "DatasetNameBase": {
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
            "auth": {
              "description": "Authentication string for the connection.",
              "type": "string",
              "example": "account_string_123"
            },
            "host": {
              ... # other common namespace properties could go here to be ref'd by implementations
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
          "required": ["uri_base", "path"] # require only these in accordance with standard URI syntax
        },
        "name": {
          "description": "The name of the dataset, unique within a namespace.",
          "type": "object",
          "properties": {
            # like namespace, outline common properties to ref, but don't need to make any required to keep flexbility
            "database": {
              ...
            },
            "schema": {
              ...
            },
            "table": {
              ...
            }
          },
          "example": "MY_DB.MY_SCHEMA.MY_TABLE"
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
              "type": "string",
              "enum": ["upper", "lower"]
            }
          },
          "required": ["case_sensitive"]
        }
      }
    },
    "JobNameBase": {
      ...
    }
  },
  "$ref": "#/$defs/DatasetNameBase"
}
```

In the above example, the Naming.json schema is provided with a detailed example for a Dataset. The schema would live in a new `spec/naming` folder under the name `NamingBase.json`. This would be analogous to the `spec/OpenLineage.json` file that outlines the `RunEvent` and other top-level OpenLineage abstractions. An example is given only for Datasets for brevity. The goal of this example is to show how properties for a `namespace` and `name` can be defined generically with a JSON schema, to be implemented by individual integrations. For a dataset, four pieces of information are needed:
  1. The `namespace`, which minimally should include a `scheme` and a `path` as this seems to be consistent with URI naming conventions, see the [syntax diagram](https://upload.wikimedia.org/wikipedia/commons/d/d6/URI_syntax_diagram.svg). The base spec can also define any URI attributes, such as `auth`/`userinfo`, `host`, or `port`, to be `$ref`'d by implementers.
  2. The `name`, whose required properties, if any, are left to discussion. Again, properties can be defined to be re-used in implementations.
  3. The `unique_name_qualifier`, which is a character or short string that will combine the `namespace` and `name`. Often, this will simply be a `/`. This may not be necessary if there's a way of generating the classes that has this rule already.
  4. The `case_sensitivity` is an object that will allow case-sensitive enforcement. This is necessary for some implementers, like Snowflake, and will ensure that all integrations correctly capitalize datasets. This field may also be unnecessary if the class generator can handle this.

A similar set of requirements can be developed for Jobs. In this example, each potential element of a `namespace` or `name` is given explicitly as an object. This has the benefit of creating re-usable properties, even if it is verbose. Additionally, some of these properties could be defined as `pattern`s instead of `string`s to enforce certain conventions.

Although one drawback with this type of schema is that the casing must have its own rule as to what fields it applies to; in the example, the `namespace` does not need the rule while the `name` does. Casing may be moved then to each of `namespace` and `name`, defined elsewhere and `$ref`'d in each property, or moved to the class generator.

[Snowflake Example]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/snowflake/1-0-0/Snowflake.json",
  "$defs": {
    "SnowflakeDataset": {
      "allOf": [{
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetNameBase"
      }],
      "type": "object",
    }
  },
  "type": "object",
  "properties": { # not entirely sure what this part is doing
    "name": {
      "$ref": "#/$defs/SnowflakeDataset"
    }
  }
}
```

In the Snowflake example above, the `SnowflakeDataset` would be implemented simply by referencing the `DatasetNameBase` schema. This simplifies the implementation greatly, but may cause some other issues.

One potential issue is that either Snowflake relies on `DatasetNameBase` to implement properties like `region` within the `namespace` property, which the `DatasetNameBase` example above does **not** do, or the `DatasetNameBase` example should implement each property outside of `namespace` and `name` (in other words, to have the list of `DatasetNameBase` properties be `uri_base`, `auth`, `database`, etc...) and have the `namespace` and `name` properties simply state how these other properties are put together. An example of this is given below:

[Base Example 2]
[Base Example]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/1-0-0/Naming.json",
  "$defs": {
    "DatasetNameBase": {
      "type": "object",
      "properties": {
        "uri_base": {
          "description": "The base of a uri that specifies where the source object came from.",
          "type": "string",
          "example": "uri://"
        },
        "auth": {
          "description": "Authentication string for the connection.",
          "type": "string",
          "example": "account_string_123"
        },
        "host": {
          ...
        },
        "port": {
          ...
        },
        "database": {
          ...
        },
        "schema": {
          ...
        },
        "table": {
          ...
        },
        "namespace": {
          "description": "The components that describe the namespace for this integration.",
          "type": "object",
          "properties": {
            # potentially ref the above properties or do an anyOf?
          },
          "additionalProperties": {"type": "string"} # using this as a means of hacking around the above issue,
          # essentially leaving properties unimplemented but required, so the implementer can fill it in as-needed
        },
        "name": {
          "description": "The name of the dataset, unique within a namespace.",
          "type": "object",
          "properties": {
            # same issue as in namesapce, do we ref or use anyOf
            # or does name, namespace, and unique_name_qualifier get moved out of
            # this object entirely and have their own Name, Namespace, etc... objects
          },
          "additionalProperties": {"type": "string"},
          "example_name": "MY_DB.MY_SCHEMA.MY_TABLE"
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
              "type": "string",
              "enum": ["upper", "lower"]
            }
          },
          "required": ["case_sensitive"]
        }
      },
      "required": ["name", "namespace"]
    },
    "JobNameBase": {
      ...
    }
  },
  "$ref": "#/$defs/DatasetNameBase"
}
```

In the case above, we flatten the hierarchy of properties in the `DatasetNameBase` to allow all sorts of basic properties that can be referenced, as in the Snowflake example below.

[Snowflake Example 2]
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://openlineage.io/spec/naming/snowflake/1-0-0/Snowflake.json",
  "$defs": {
    "SnowflakeDataset": {
      "allOf": [{
        "$ref": "https://openlineage.io/spec/naming/1-0-0/Naming.json#/$defs/DatasetNameBase"
      }, {
        "type": "object",
        "properties": {
          "region": {...},
          "warehouse": {...}
        },
        "required": ["warehouse"]
      }],
      "type": "object",
    }
  },
  "type": "object",
  "properties": { # not entirely sure what this part is doing
    "name": {
      "$ref": "#/$defs/SnowflakeDataset"
    }
  }
}
```

In this Snowflake schema, we can add the properties relevant only to Snowflake and require ones that are absolutely necessary. The open question here is how to define the correct `name` and `namespace` in this subschema, as the author is unsure how. Perhaps in this method, `name` and `namespace` simply need to be their own schemas that are part of the `allOf`.

Another possibility is to only specify `namespace` and `name` schemas that the `main/spec/OpenLineage.json` defs can then reference, as `Datasets` and `Jobs` are already defined there. Then these `namespace` and `name` schemas can be defined very precisely, with each integration getting more specific, and a `Dataset` can be updated to take one of these more specific schemas. This may be much simpler than the above.

Ultimately, these JSON schemas should get created into classes with various attributes and validation.

In addition to the documents themselves, some testing framework should be developed (or tests simply added to integrations) to ensure that naming matches the structure and casing of the JSON in the files exactly. This should include a validation framework available in the client that integrations can run against.
