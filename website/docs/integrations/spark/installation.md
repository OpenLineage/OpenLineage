---
sidebar_position: 2
title: Installation
---

To integrate OpenLineage Spark with your application, you can:

- [Bundle the package with your Apache Spark application project](#bundle-the-package-with-your-apache-spark-application-project).
- [Place the JAR in your `${SPARK_HOME}/jars` directory](#place-the-jar-in-your-spark_homejars-directory)
- [Use the `--jars` option with `spark-submit / spark-shell / pyspark`](#use-the---jars-option-with-spark-submit--spark-shell--pyspark)
- [Use the `--packages` option with `spark-submit / spark-shell / pyspark`](#use-the---packages-option-with-spark-submit--spark-shell--pyspark)

#### Bundle the package with your Apache Spark application project

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](configuration/usage.md) section.
:::

For Maven, add the following to your `pom.xml`:


```xml
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>openlineage-spark_${SCALA_BINARY_VERSION}</artifactId>
  <version>{{PREPROCESSOR:OPENLINEAGE_VERSION}}</version>
</dependency>
```

For Gradle, add this to your `build.gradle`:

```groovy
implementation("io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:{{PREPROCESSOR:OPENLINEAGE_VERSION}}")
```

#### Place the JAR in your `${SPARK_HOME}/jars` directory

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](#configuration) section.
:::

1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification, move the JAR to `${SPARK_HOME}/jars`.

This script automates the download and verification process:


```bash
#!/usr/bin/env bash

if [ -z "$SPARK_HOME" ]; then
    echo "SPARK_HOME is not set. Please define it as your Spark installation directory."
    exit 1
fi

OPENLINEAGE_SPARK_VERSION='{{PREPROCESSOR:OPENLINEAGE_VERSION}}'
SCALA_BINARY_VERSION='2.13'        # Example Scala version
ARTIFACT_ID="openlineage-spark_${SCALA_BINARY_VERSION}"
JAR_NAME="${ARTIFACT_ID}-${OPENLINEAGE_SPARK_VERSION}.jar"
CHECKSUM_NAME="${JAR_NAME}.sha512"
BASE_URL="https://repo1.maven.org/maven2/io/openlineage/${ARTIFACT_ID}/${OPENLINEAGE_SPARK_VERSION}"

curl -O "${BASE_URL}/${JAR_NAME}"
curl -O "${BASE_URL}/${CHECKSUM_NAME}"

echo "$(cat ${CHECKSUM_NAME})  ${JAR_NAME}" | sha512sum -c

if [ $? -eq 0 ]; then
    mv "${JAR_NAME}" "${SPARK_HOME}/jars"
else
    echo "Checksum verification failed."
    exit 1
fi
```

#### Use the `--jars` option with `spark-submit / spark-shell / pyspark`

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](#configuration) section.
:::

1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification, submit a Spark application with the JAR using the `--jars` option.

This script demonstrate this process:

```bash
#!/usr/bin/env bash

OPENLINEAGE_SPARK_VERSION='{{PREPROCESSOR:OPENLINEAGE_VERSION}}'
SCALA_BINARY_VERSION='2.13'        # Example Scala version
ARTIFACT_ID="openlineage-spark_${SCALA_BINARY_VERSION}"
JAR_NAME="${ARTIFACT_ID}-${OPENLINEAGE_SPARK_VERSION}.jar"
CHECKSUM_NAME="${JAR_NAME}.sha512"
BASE_URL="https://repo1.maven.org/maven2/io/openlineage/${ARTIFACT_ID}/${OPENLINEAGE_SPARK_VERSION}"

curl -O "${BASE_URL}/${JAR_NAME}"
curl -O "${BASE_URL}/${CHECKSUM_NAME}"

echo "$(cat ${CHECKSUM_NAME})  ${JAR_NAME}" | sha512sum -c

if [ $? -eq 0 ]; then
    spark-submit --jars "path/to/${JAR_NAME}" \
      # ... other options
else
    echo "Checksum verification failed."
    exit 1
fi
```

#### Use the `--packages` option with `spark-submit / spark-shell / pyspark`

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](#configuration) section.
:::

Spark allows you to add packages at runtime using the `--packages` option with `spark-submit`. This
option automatically downloads the package from Maven Central (or other configured repositories)
during runtime and adds it to the classpath of your Spark application.

```bash
OPENLINEAGE_SPARK_VERSION='{{PREPROCESSOR:OPENLINEAGE_VERSION}}'
SCALA_BINARY_VERSION='2.13'        # Example Scala version

spark-submit --packages "io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:{{PREPROCESSOR:OPENLINEAGE_VERSION}}" \
    # ... other options
```

:::warning
Version `1.8.0` and earlier only supported Scala 2.12 variants of Apache Spark. 
Scala version name was not included in the artifact identifier.
:::

