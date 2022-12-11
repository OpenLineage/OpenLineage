# OpenLineage SQL Parser

A library that extracts lineage from SQL statements. 

## Usage

If you're using OpenLineage integration, there's good chance that you're already using this integration.

This library is implemented in Rust and provides a Python and Java interface. The Rust implementation has not yet been published to Cargo.
The interface is explained in INTERFACE.md.

## Installation

### Python

```bash
$ pip install openlineage-sql 
```

To install from source, you need to have a Rust toolchain.

```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
```

You can run Cargo tests then:

```bash
cargo test
```

To build a Python wheel for your system and architecture, you need a Maturin build system.
It's recommended to install this in a virtualenv.

```bash
cd iface-py
python -m pip install maturin
maturin build --out ../target/wheels
```

You can verify that the library has been properly built by running:

```bash
pip install openlineage-sql --no-index --find-links ../target/wheels --force-reinstall
python -c "import openlineage_sql"
```

### Java

To build the Java interface run the following script from the project root:

```bash
./iface-java/script/build.sh
```

This produces an `openlineage-sql.jar` in the `iface-java/target` directory.

The interface can be manually tested by running the integration test from the `iface-java` directory. When no arguments are provided, the test runs in interactive mode.

```bash
./tests/integration/run_test.sh [sql]
```

#### Todo:
* Support a larger part of the SQL language 
* Python as a Cargo feature
* Explore a Java integration

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2022 contributors to the OpenLineage project
