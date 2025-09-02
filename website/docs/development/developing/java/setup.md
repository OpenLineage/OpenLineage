---
title: Setup a development environment
sidebar_position: 1
---

There are multiple Java based modules in OpenLineage, two of which you'll often have to build in order to work with other modules (integrations):
* `openlineage-java` — SDK for Java programming language for generating and emitting OpenLineage events to OpenLineage backends.
* `openlineage-sql-java` — Java interface for OpenLineage SQL Parser written in Rust

This page covers the base setup. If a module requires anything additional, refer to their respective documentation (e.g. [openlineage-spark](https://openlineage.io/docs/development/developing/spark/setup))

## JDK

To work with Java modules in OpenLineage, JDK 17 is required.

You can verify your installation by running:
```bash
java --version && javac --version
```

Both tools should show version 17.X.X. If the commands are not found or are on a different version, install a correct version and make sure it is on your `PATH`.
Tools like SDKMAN! can be used to simplify the installation process.

## C Compiler

`openlineage-sql-java` module is almost always a dependency for integrations. The SQL parser it contains is written in Rust,
and it requires a C Compiler for the compilation process.
To verify you have CC installed run:
```bash
cc --version
```