---
sidebar_position: 2
title: Installation
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::warning

* Version `1.8.0` and earlier only supported Scala 2.12 variants of Apache Spark.
* Version `1.9.1` and later support both Scala 2.12 and 2.13 variants of Apache Spark.

The above necessitates a change in the artifact identifier for `io.openlineage:openlineage-spark`.
After version `1.8.0`, the artifact identifier has been updated. For subsequent versions, utilize:
`io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:${OPENLINEAGE_SPARK_VERSION}`.
:::

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

<Tabs groupId="spark">
<TabItem value="after-1.8.0" label="After 1.8.0">

```xml
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>openlineage-spark_${SCALA_BINARY_VERSION}</artifactId>
  <version>${OPENLINEAGE_SPARK_VERSION}</version>
</dependency>
```

</TabItem>
<TabItem value="1.8.0-and-earlier" label="1.8.0 and earlier">

```xml
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>openlineage-spark</artifactId>
  <version>${OPENLINEAGE_SPARK_VERSION}</version>
</dependency>
```

</TabItem>
</Tabs>

For Gradle, add this to your `build.gradle`:

<Tabs groupId="spark">
<TabItem value="after-1.8.0" label="After 1.8.0">

```groovy
implementation("io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:${OPENLINEAGE_SPARK_VERSION}")
```

</TabItem>
<TabItem value="1.8.0-and-earlier" label="1.8.0 and earlier">

```groovy
implementation("io.openlineage:openlineage-spark:${OPENLINEAGE_SPARK_VERSION}")
```

</TabItem>
</Tabs>

#### Place the JAR in your `${SPARK_HOME}/jars` directory

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](#configuration) section.
:::

1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification, move the JAR to `${SPARK_HOME}/jars`.

This script automates the download and verification process:

<Tabs groupId="spark">
<TabItem value="after-1.8.0" label="After 1.8.0">

```bash
#!/usr/bin/env bash

if [ -z "$SPARK_HOME" ]; then
    echo "SPARK_HOME is not set. Please define it as your Spark installation directory."
    exit 1
fi

OPENLINEAGE_SPARK_VERSION='1.9.0'  # Example version
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

</TabItem>
<TabItem value="1.8.0-and-earlier" label="1.8.0 and earlier">

```bash
#!/usr/bin/env bash

if [ -z "$SPARK_HOME" ]; then
    echo "SPARK_HOME is not set. Please define it as your Spark installation directory."
    exit 1
fi

OPENLINEAGE_SPARK_VERSION='1.8.0'  # Example version
ARTIFACT_ID="openlineage-spark"
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

</TabItem>
</Tabs>

#### Use the `--jars` option with `spark-submit / spark-shell / pyspark`

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](#configuration) section.
:::

1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification, submit a Spark application with the JAR using the `--jars` option.

This script demonstrate this process:

<Tabs groupId="spark">
<TabItem value="after-1.8.0" label="After 1.8.0">

```bash
#!/usr/bin/env bash

OPENLINEAGE_SPARK_VERSION='1.9.0'  # Example version
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

</TabItem>
<TabItem value="1.8.0-and-earlier" label="1.8.0 and earlier">

```bash
#!/usr/bin/env bash

OPENLINEAGE_SPARK_VERSION='1.8.0'  # Example version
ARTIFACT_ID="openlineage-spark"
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

</TabItem>
</Tabs>

#### Use the `--packages` option with `spark-submit / spark-shell / pyspark`

:::info
This approach does not demonstrate how to configure the `OpenLineageSparkListener`.
Please refer to the [Configuration](#configuration) section.
:::

Spark allows you to add packages at runtime using the `--packages` option with `spark-submit`. This
option automatically downloads the package from Maven Central (or other configured repositories)
during runtime and adds it to the classpath of your Spark application.

<Tabs groupId="spark">
<TabItem value="after-1.8.0" label="After 1.8.0">

```bash
OPENLINEAGE_SPARK_VERSION='1.9.0'  # Example version
SCALA_BINARY_VERSION='2.13'        # Example Scala version

spark-submit --packages "io.openlineage:openlineage-spark_${SCALA_BINARY_VERSION}:${OPENLINEAGE_SPARK_VERSION}" \
    # ... other options
```

</TabItem>
<TabItem value="1.8.0-and-earlier" label="1.8.0 and earlier">

```bash
OPENLINEAGE_SPARK_VERSION='1.8.0'  # Example version

spark-submit --packages "io.openlineage:openlineage-spark::${OPENLINEAGE_SPARK_VERSION}" \
    # ... other options
```

</TabItem>
</Tabs>