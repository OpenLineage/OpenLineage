# AGENTS.md

Guidelines for AI coding agents contributing to OpenLineage.

## Protected paths

**Do NOT modify these files without explicit user authorization:**

- `spec/OpenLineage.json`
- `spec/OpenLineage.yml`
- `spec/facets/*.json`
- `spec/registry/**/*.json`

Before modifying any spec file, ask: "This change affects the OpenLineage specification. Do you authorize this modification?"

## Spec changes

All spec changes MUST be backwards compatible per [SchemaVer](spec/Versioning.md):

- **Safe**: Adding optional fields, new facets, new enum values
- **Unsafe**: Removing fields, changing types, making fields required, renaming

When changing the spec:
1. Update version in `$id` field appropriately
2. Add test cases in `spec/tests/`
3. Regenerate Python client: `python ./client/python/src/openlineage/client/generator/generate.py`
4. Coordinate changes across all clients (Java, Python)

## Setup commands

- Install pre-commit hooks: `prek install`
- Run all checks: `prek run --all-files`

## Build commands

- Java client: `cd client/java && ./gradlew build`
- Python client: `cd client/python && uv sync && uv run pytest`

## Testing

All code changes require tests:

- Spec: `spec/tests/{FacetName}/`
- Java: `client/java/src/test/`
- Python: `client/python/tests/`

## Commit conventions

Always sign off commits (DCO required):

```bash
git commit -s -m "component: description"
```

Components: `spec`, `client/java`, `client/python`, `spark`, `flink`, `dbt`, `airflow`, `docs`, `ci`, `build`

## AI disclosure

When AI assistants contribute to code, disclose this in commit messages:

```
Co-Authored-By: AI-Assistant-Name <noreply@example.com>
```

Examples:
```
Co-Authored-By: Claude <noreply@anthropic.com>
Co-Authored-By: GitHub Copilot <noreply@github.com>
Co-Authored-By: Cursor <noreply@cursor.com>
```

## Code style

- Java: Spotless formatter
- Python: ruff (PEP 8)
- JSON/YAML: prettier

## License headers

All new files need Apache 2.0 headers:

```java
/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
```

```python
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
```

## Client coordination

Changes affecting client functionality must be coordinated across all clients:

- `client/java/` - Java client
- `client/python/` - Python client

Examples: new transports, new facet support, API changes, configuration options.

## PR guidelines

- One logical change per PR
- Keep diffs small and focused
- Link related issues with `closes: #123`
- Update documentation with code changes

## Repository structure

```
spec/           # PROTECTED - OpenLineage specification
client/java/    # Java client
client/python/  # Python client
integration/    # Spark, Flink, dbt, Airflow integrations
proposals/      # Design proposals
website/        # Documentation
```

## References

- [CONTRIBUTING.md](CONTRIBUTING.md) - Full contribution guidelines
- [spec/Versioning.md](spec/Versioning.md) - Version numbering rules
- [why-the-dco.md](why-the-dco.md) - DCO explanation
