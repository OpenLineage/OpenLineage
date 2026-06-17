---
title: Setup a development environment
sidebar_position: 1
---

There are four Python OpenLineage packages that you can install locally when setting up a development environment:<br />
[openlineage-python](https://pypi.org/project/openlineage-python/) (client), [openlineage-sql](https://pypi.org/project/openlineage-sql/) and [openlineage-integration-common](https://pypi.org/project/openlineage-integration-common/).

The repository uses [UV](https://docs.astral.sh/uv/) for Python dependency management with path-based dependencies, where each integration is a standalone project with isolated dependencies.

## Prerequisites

Install UV if you haven't already:

```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Quick Start with Task

The repository includes [Task](https://taskfile.dev/installation/) files to simplify the development environment setup. From the repository root:

```bash
# View all available commands
$ task --list

# Setup specific packages
$ task clients:python:setup       # Python client
$ task integrations:common:setup  # Integration common library
$ task integrations:dbt:setup     # dbt integration

# Run tests
$ task python:test                # Test all Python packages
$ task clients:python:test        # Test a specific package

# Run linting and type checking
$ task python:lint                # Check formatting and lint (ruff)
$ task python:format              # Auto-fix formatting issues (ruff)
$ task python:typecheck           # Run mypy across all Python packages

# Clean all virtual environments
$ task python:clean
```

Each package also has its own `Taskfile.yml`, so inside e.g. `client/python` you can simply run `task setup`, `task test`, or `task typecheck`.

## Manual Setup

If you prefer to set up integrations manually:

```bash
# Python client
$ cd client/python
$ uv sync --extra dev --extra test

# Integration common
$ cd integration/common
$ uv sync --extra dev

# dbt integration
$ cd integration/dbt
$ uv sync --extra dev
```

## How Path-Based Dependencies Work

The repository uses path-based dependencies instead of a UV workspace because each integration has potentially conflicting dependencies. Each integration is a standalone project with its own isolated virtual environment.

Each integration automatically installs its dependencies from local directories in editable mode:
- dbt integration depends on `common` package
- Common integration depends on `client` and `sql` packages

UV handles these path-based dependencies automatically, so changes in one package are immediately reflected in dependent packages without reinstallation.

## Testing

There are unit tests available for OpenLineage Python libraries. You can run them with a simple `pytest` command with directory set to library base path.
