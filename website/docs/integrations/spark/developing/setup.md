---
sidebar_position: 1
title: Setup
---

## Preparation

The integration expects `openlineage-java`, `spark-extension-interfaces` and `openlineage-sql-java` to exist in
your local maven repository, so before building/testing the Spark integration you need to first build and publish
the required dependencies.

You can do it using [Task](https://taskfile.dev/installation/):

```sh
task setup
```

## Testing

To run the tests, from the current directory run:

```sh
task test
```

To run the integration tests (requires Docker), from the current directory run:

```sh
task integration-test
```

## Build jar

```sh
task jar
```