---
sidebar_position: 1
title: Setup
---

## Preparation

The integration depends on four libraries that are build locally `openlineage-java`, `spark-extension-interfaces`, `spark-extension-entrypoint` and `openlineage-sql-java`,
so before any testing or building of a package you need to publish the appropriate artifacts to local maven repository.
To build the packages you need to execute:

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