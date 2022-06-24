# OpenLineage SQL Parser

Library that extracts lineage from sql statements. 

### Usage

If you're using OpenLineage integration, there's good chance that you're already using this integration.

This library can be used both as Python library and as Rust library, however it's not published at Cargo yet.

### Installation

Python:

```bash
$ pip install openlineage-sql 
```

To install from source, you need to have Rust toolchain.

```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
```

You can run cargo tests then:

```bash
cargo test
```

To build Python wheel for your system and architecture you need Maturin build system.
It's recommended to install this in virtualenv.

```bash
python -m pip install maturin
maturin build --out target/wheels
```

You can verify that library has been properly build by running

```bash
pip install openlineage-sql --no-index --find-links target/wheels --force-reinstall
python -c "import openlineage_sql"
```

#### Todo:
* Support larger part of SQL language 
* Column-level lineage
* Python as a cargo feature
* Explore Java integration