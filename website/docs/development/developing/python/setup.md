---
title: Setup a development environment
sidebar_position: 1
---

There are four Python OpenLineage packages that you can install locally when setting up a development environment:<br />
[openlineage-python](https://pypi.org/project/openlineage-python/) (client), [openlineage-sql](https://pypi.org/project/openlineage-sql/), [openlineage-integration-common](https://pypi.org/project/openlineage-integration-common/), and [openlineage-airflow](https://pypi.org/project/openlineage-airflow/).

The repository uses [UV](https://docs.astral.sh/uv/) for Python dependency management with path-based dependencies, where each integration is a standalone project with isolated dependencies.

## Prerequisites

Install UV if you haven't already:

```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Quick Start with Makefile

The repository includes a Makefile to simplify the development environment setup:

```bash
# View all available commands
$ make help

# Setup all Python integrations at once
$ make setup-all

# Or setup specific integrations
$ make setup-client      # Python client
$ make setup-common      # Integration common library
$ make setup-airflow     # Airflow integration
$ make setup-dbt         # dbt integration

# Run tests
$ make test-all          # Test all integrations
$ make test-client       # Test specific integration

# Run linting and type checking
$ make lint-all          # Run all linting
$ make fix-format        # Auto-fix formatting issues

# Check status of your setup
$ make status

# Clean all virtual environments
$ make clean
```

## Manual Setup

If you prefer to set up integrations manually:

```bash
# Python client
$ cd client/python
$ uv sync --extra dev --extra test

# Integration common
$ cd integration/common
$ uv sync --extra dev

# Airflow integration
$ cd integration/airflow
$ uv sync --extra dev --extra airflow

# dbt integration
$ cd integration/dbt
$ uv sync --extra dev
```

## How Path-Based Dependencies Work

The repository uses path-based dependencies instead of a UV workspace because each integration has potentially conflicting dependencies. Each integration is a standalone project with its own isolated virtual environment.

Each integration automatically installs its dependencies from local directories in editable mode:
- Airflow integration depends on `client`, `common`, and `sql` packages
- dbt integration depends on `common` package
- Common integration depends on `client` and `sql` packages

UV handles these path-based dependencies automatically, so changes in one package are immediately reflected in dependent packages without reinstallation.