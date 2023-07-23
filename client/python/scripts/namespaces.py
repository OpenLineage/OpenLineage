# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import os
import sys

import attrs
import yaml


@attrs.define
class Format:
    form: str
    arguments: list[str]


@attrs.define
class Definition:
    name: str
    parts: list[str]
    form: Format


def parse_definition(content: dict) -> Definition:
    return Definition(**content)


def parse_in_directory(path: str) -> list[Definition]:
    definitions = []
    files = [
        os.path.join(path, f)
        for f in os.listdir(path)
        if f is not None and os.path.isfile(os.path.join(path, f)) and f.endswith((".yml", ".yaml"))
    ]
    for file in files:
        with open(file) as f:
            definitions.append(parse_definition(yaml.safe_load(f)))
    return definitions


def generate_definitions(definitions: list[Definition]) -> str:
    defs = []
    for df in definitions:
        method_args = ", ".join(df.parts)
        format_args = ", ".join(df.form.arguments)
        defs.append(
            f'def from{df.name}({method_args}):\n    return "{df.form.form}" % ({format_args})',
        )
    return "\n\n".join(defs)


def generate() -> None:
    argcount = 2
    if len(sys.argv) != argcount:
        msg = f"Wrong number of arguments given to namespaces.py. Should be 2, got {len(sys.argv)}"
        raise RuntimeError(
            msg,
        )
    spec_path = sys.argv[1]
    if not os.path.isdir(spec_path):
        msg = f"Path {spec_path} is not a path to directory"
        raise RuntimeError(msg)
    definitions = parse_in_directory(spec_path)
    generate_definitions(definitions)


if __name__ == "__main__":
    print(  # noqa: T201
        generate_definitions(
            [
                Definition(
                    name="ArgLocator",
                    parts=["arg1", "asdf"],
                    form=Format(form="snowflake://%s/%s", arguments=["asdf", "arg1"]),
                ),
                Definition(
                    name="Dupa",
                    parts=["a"],
                    form=Format(form="postgres://%s", arguments=["a"]),
                ),
            ],
        ),
    )
