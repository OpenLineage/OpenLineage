# OpenLineage SQL Parser

A library that extracts lineage from SQL statements. 

### Usage

If you're using OpenLineage integration, there's good chance that you're already using this integration.

This library can be used both as Python library and as Rust library; however, it's not published at Cargo yet.

### Installation

Python:

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
python -m pip install maturin
maturin build --out target/wheels
```

You can verify that the library has been properly built by running:

```bash
pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall
python -c "import openlineage_sql"
```

#### Todo:
* Support a larger part of the SQL language 
* Python as a Cargo feature
* Explore a Java integration