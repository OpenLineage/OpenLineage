# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pathlib
import tempfile

import click
import httpx
from openlineage.client.generator.base import (
    camel_to_snake,
    format_and_save_output,
    load_specs,
    parse_and_generate,
)

BASE_SPEC_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"


def get_base_spec() -> pathlib.Path:
    """Get the base spec from the OpenLineage repository."""
    response = httpx.get(BASE_SPEC_URL, timeout=10)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    tmpfile = pathlib.Path(tempfile.mkdtemp()) / "OpenLineage.json"
    tmpfile.write_text(response.text)
    return tmpfile


@click.command()
@click.argument("facets_spec_location", type=pathlib.Path)
@click.option("--output-location", type=pathlib.Path, default=None)
def main(facets_spec_location: pathlib.Path, output_location: pathlib.Path | None) -> None:
    base_spec_location = get_base_spec()
    locations = load_specs(base_spec_location=base_spec_location, facets_spec_location=facets_spec_location)
    results = parse_and_generate(locations)
    if not output_location:
        output_location = pathlib.Path(tempfile.mkdtemp())
    modules = {
        output_location.joinpath(*[camel_to_snake(n.replace("Facet", "")) for n in name]): result.body
        for name, result in sorted(results.items())
    }
    for path, body in modules.items():
        if path and not path.parent.exists():
            path.parent.mkdir(parents=True)
        if body and path.name not in ("open_lineage.py", "__init__.py"):
            format_and_save_output(body, path, path.name == "open_lineage.py")
        if path.name not in ("open_lineage.py", "__init__.py"):
            with open(path) as file:
                print(f"Generator would generate file {path.name}:\r\n\r\n{file.read()}")


if __name__ == "__main__":
    main()
