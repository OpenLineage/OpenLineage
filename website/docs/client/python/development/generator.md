---
sidebar_position: 3
title: Generator CLI Tool
---

## Generator CLI Tool

The Python client includes a CLI tool that allows you to generate Python classes from OpenLineage specification files. This is particularly useful if you want to:

- Create custom facets based on your own JSON schema definitions
- Generate client code that matches a specific version of the OpenLineage specification
- Extend the OpenLineage model with domain-specific classes

### Dependencies

The CLI tool requires `datamodel-code-generator`, a library that converts JSON Schema to Python data models. If you plan to use the generator, install it with:

```bash
pip install "openlineage-python[generator]"
```

### Usage

```bash
ol-generate-code [FACETS_SPEC_LOCATION] [--output-location OUTPUT_LOCATION]
```

#### Arguments

- `FACETS_SPEC_LOCATION`: Path to a JSON file or directory containing JSON files with OpenLineage facet specifications
- `--output-location`: (Optional) Directory where the generated Python classes will be saved. If not specified, output will be printed to stdout with proposed file names.

#### Examples

Generate Python classes from a single facet specification file:

```bash
ol-generate-code my_custom_facet.json --output-location ./generated_code
```

Generate Python classes from a directory containing multiple facet specification files:

```bash
ol-generate-code ./facets_dir --output-location ./generated_code
```

### How It Works

The CLI tool:

1. Retrieves the base OpenLineage specification from `https://openlineage.io/spec/2-0-2/OpenLineage.json`
2. Loads and parses your custom facet specifications
3. Uses the `datamodel-code-generator` library to generate Python classes that match the structure of the specifications
4. Formats the generated code using Ruff. The generator automatically converts camelCase names to snake_case for Python conventions
5. Outputs the files to the specified location
