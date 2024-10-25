---
sidebar_position: 6
---

# Environment Variables Run Facet
The Environment Variables Run Facet provides detailed information about the environment variables that were set during the execution of a job. This facet is useful for capturing the runtime environment configuration, which can be used for categorizing and filtering jobs based on their environment settings.

| Property              | Description                                                                 | Type   | Example                   | Required |
|-----------------------|-----------------------------------------------------------------------------|--------|---------------------------|----------|
| name                  | The name of the environment variable. This helps in identifying the specific environment variable used during the job run. | string | "JAVA_HOME"              | Yes      |
| value                 | The value of the environment variable. This captures the actual value set for the environment variable during the job run. | string | "/usr/lib/jvm/java-11"   | Yes      |

The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/EnvironmentVariablesRunFacet.json).