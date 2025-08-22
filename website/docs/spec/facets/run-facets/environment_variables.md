---
sidebar_position: 1
---

# Environment Variables Run Facet
The Environment Variables Run Facet provides detailed information about the environment variables that were set during the execution of a job. This facet is useful for capturing the runtime environment configuration, which can be used for categorizing and filtering jobs based on their environment settings.

Fields:

- `environmentVariables`: The environment variables for the run, collected by OpenLineage. Array of objects, the order doesn't matter:
  - `name`: The name of the environment variable.
  - `value`: The value of the environment variable.

Example:

```json
{
    ...
    "run": {
        "facets": {
            "environmentVariables": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/main/spec/facets/EnvironmentVariablesRunFacet.json",
                "environmentVariables": [
                    {
                        "name": "JAVA_HOME",
                        "value": "/usr/lib/jvm/java-11-openjdk"
                    },
                    {
                        "name": "PATH",
                        "value": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                    }
                ]
            }
        }
    }
    ...
}
```

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/EnvironmentVariablesRunFacet.json).
