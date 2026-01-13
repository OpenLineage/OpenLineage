---
sidebar_position: 1
title: Setup
---

## Preparation

The integration expects `openlineage-java`, `spark-extension-interfaces` and `openlineage-sql-java` to exist in
your local maven repository, so before building/testing the Spark integration you need to first build and publish
the required dependencies.

You can do it using the convenience script:

```sh
./buildDependencies.sh
```

## Testing

To run the tests, from the current directory run:

```sh
./gradlew test
```

To run the integration tests, from the current directory run:

```sh
./gradlew integrationTest
```

## Build jar

```sh
./gradlew shadowJar
```