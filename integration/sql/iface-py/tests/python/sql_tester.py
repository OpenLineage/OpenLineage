# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import argparse
import os
import sys

from openlineage_sql import parse


class bcolors:  # noqa: N801
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


# Usage: simply enter SQL statement from the input prompt or echo / cat SQL statement
# via UNIX pipe to the tool. In case of inputing directly, make sure to press
# Ctrl+D after the input to EOF the input.
def main():
    parser = argparse.ArgumentParser(description="Simple tester to parse and test SQL statements.")
    parser.add_argument("--dialect", nargs="?", default=None, help="set the SQL dialect.")

    args = parser.parse_args()
    # dialect is None if no dialect was specified.
    dialect = args.dialect

    k = 0
    sql = ""

    if os.isatty(0):
        print(bcolors.OKGREEN + "Welcome to OpenLineage SQL Parser Tester." + bcolors.ENDC)
        if dialect is not None:
            print(f"Dialect selected : {dialect}")
        print("Enter SQL statement, and press Ctrl+D at the last line when finished.\n> ", end="")

    try:
        for line in sys.stdin:
            k += 1
            sql += line
    except KeyboardInterrupt:
        pass

    if sql != "":
        meta = parse([sql]) if dialect is None else parse([sql], dialect=dialect)
        print("\n" + bcolors.OKBLUE + "------ OpenLineage SQL Parser Tester ------" + bcolors.ENDC)
        print(f"> SQL received:\n{sql}")
        if meta is not None:
            print_meta(meta)


def print_meta(metadata):
    in_tables = metadata.in_tables
    out_tables = metadata.out_tables
    print(bcolors.OKCYAN + "----- SQL META ------" + bcolors.ENDC)
    print(f"> IN: there are {len(in_tables)} tables")
    for tablemeta in in_tables:
        print_table(tablemeta)
    print(f"> OUT: there are {len(out_tables)} tables")
    for tablemeta in out_tables:
        print_table(tablemeta)


def print_table(tablemeta):
    print(f"- database: {tablemeta.database}")
    print(f"  schema: {tablemeta.schema}")
    print(f"  name: {tablemeta.name}")
    print(f"  qualified name: {tablemeta.qualified_name}")


if __name__ == "__main__":
    main()
