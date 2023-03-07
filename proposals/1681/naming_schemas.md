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

```json
{
    "integration_name": {
        "type": "Job" | "Dataset",
        "namespace": [
            "uri_base",
            "auth",
            "host",
            "port"
        ],
        "name": [
            "db",
            "schema",
            "table"
        ],
        "example_namespace": "uri://auth.host:port",
        "example_name": "MY_DB.MY_SCHEMA.MY_TABLE",
        "example_unique_name": "uri://auth.host:port/MY_DB.MY_SCHEMA.MY_TABLE",
        "case_sensitive": true | false,
        "case": "upper" | "lower" | null
  }
}
```

In the above outline, the `integration_name` would match the name of the file. The `type` is one of "Job" or "Dataset". The `namespace` list contains all the elements, in order, needed to generate the namespace (this may be an issue if, like Redshift, there are multiple options. This might necessitate two files, one for each namespace). The `name` field is similar, but for elements to generate the name of the entity. Three examples are given, one for the namespace, another for the name, and a last for the unique name as a combination of the two, for clarity. Finally, a note about case sensitivity is included to ensure that all integrations are completely matching the spec.

Although one drawback with this type of schema is that the casing must have its own rule as to what fields it applies to; in the example, the `namespace` does not need the rule while the `name` does.

Another potential json schema could look like:

```json
{
    "integration_name": {
        "type": "Job" | "Dataset",
        "namespace": "^(?<uri>[A-Za-z]+://)(?<auth>[A-Za-z0-9]*)\.(?<host>[A-Za-z0-9]+):(?<port>[0-9]{1,6})$",
        "name": "^(?<database>[A-Z]+)\.(?<schema>[A-Z]+)\.(?<table>[A-Z]+)$",
        "unique_name": "$namespace/$name"
  }
}
```

In the above outline, regular expressions replace the `namespace` and `name` lists. This also negates the need for examples, as the required input is self-evident (or at least as evident as regexes ever are). The regular expressions in each of `namespace` and `name` have named matching groups for each componenet, and are themselves referenced in whole by `unique_name`, which provides the rule for combining these expressions. Finally, the two fields to determine casing are removed, as the regular expression conveys the necessary casing rules.

A major drawback of using regular expressions here is the rigidity of the expression: real-world input is messy, and restrictive expressions will likely break on legitimate cases. 

Both of the above examples also do not allow for multiple correct `unique_names`, for instance, if a warehouse accepts a URI both with and without a region, both of the above would only accept cases without the region. This may be solved by either having multiple files for these cases, or, in the regex example, have a list of valid regexes in `namespace` or `name`.

In addition to the documents themselves, some testing framework should be developed (or tests simply added to integrations) to ensure that naming matches the structure and casing of the JSON in the files exactly.
