# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import pytest
from openlineage.common.provider.dbt.utils import CONSUME_STRUCTURED_LOGS_COMMAND_OPTION
from openlineage.common.utils import (
    IncrementalFileReader,
    add_command_line_arg,
    add_or_replace_command_line_option,
    get_from_nullable_chain,
    parse_multiple_args,
    parse_single_arg,
    remove_command_line_option,
)


def test_nullable_chain_fails():
    x = {"first": {"second": {}}}
    assert get_from_nullable_chain(x, ["first", "second", "third"]) is None


def test_nullable_chain_works():
    x = {"first": {"second": {"third": 42}}}
    assert get_from_nullable_chain(x, ["first", "second", "third"]) == 42

    x = {"first": {"second": {"third": 42, "fourth": {"empty": 56}}}}
    assert get_from_nullable_chain(x, ["first", "second", "third"]) == 42


def test_parse_single_arg_does_not_exist():
    assert parse_single_arg(["dbt", "run"], ["-t", "--target"]) is None
    assert parse_single_arg(["python", "main.py", "--random_arg", "yes"], ["--what"]) is None


def test_parse_single_arg_next():
    assert parse_single_arg(["dbt", "run", "--target", "prod"], ["-t", "--target"]) == "prod"
    assert parse_single_arg(["python", "--random=yes", "--what", "asdf"], ["--what"]) == "asdf"


def test_parse_single_arg_equals():
    assert parse_single_arg(["dbt", "run", "--target=prod"], ["-t", "--target"]) == "prod"
    assert parse_single_arg(["python", "--random=yes", "--what=asdf"], ["--what"]) == "asdf"


def test_parse_single_arg_gets_first_key():
    assert parse_single_arg(["dbt", "run", "--target=prod", "-t=a"], ["-t", "--target"]) == "a"


def test_parse_single_arg_default():
    assert parse_single_arg(["dbt", "run"], ["-t", "--target"]) is None
    assert parse_single_arg(["dbt", "run"], ["-t", "--target"], default="prod") == "prod"


def test_parse_multiple_args():
    assert parse_multiple_args(["dbt", "run", "--foo", "bar"], ["-m", "--model", "--models"]) == []
    assert sorted(
        parse_multiple_args(
            [
                "dbt",
                "run",
                "--foo",
                "bar",
                "--models",
                "model1",
                "model2",
                "-m",
                "model3",
                "--something",
                "else",
                "--model=model4",
            ],
            ["-m", "--model", "--models"],
        )
    ) == ["model1", "model2", "model3", "model4"]


@pytest.mark.parametrize(
    "command_line, arg_name, arg_value, expected_command_line",
    [
        (
            ["dbt", "run", "--select", "orders"],
            "--log-format",
            "json",
            ["dbt", "run", "--select", "orders", "--log-format", "json"],
        ),
        (
            ["dbt", "run", "--select", "orders", "--log-format", "text"],
            "--log-format",
            "json",
            ["dbt", "run", "--select", "orders", "--log-format", "json"],
        ),
    ],
    ids=["add_new_arg", "replace_arg_value"],
)
def test_add_command_line_arg(command_line, arg_name, arg_value, expected_command_line):
    actual_command_line = add_command_line_arg(command_line, arg_name, arg_value)
    assert actual_command_line == expected_command_line


@pytest.mark.parametrize(
    "command_line, option, replace_option, expected_command_line",
    [
        (
            ["dbt", "run", "--select", "orders"],
            "--write-json",
            None,
            ["dbt", "run", "--select", "orders", "--write-json"],
        ),
        (
            ["dbt", "run", "--select", "orders", "--no-write-json"],
            "--write-json",
            "--no-write-json",
            ["dbt", "run", "--select", "orders", "--write-json"],
        ),
        (
            ["dbt", "run", "--select", "orders"],
            "--write-json",
            "--no-write-json",
            ["dbt", "run", "--select", "orders", "--write-json"],
        ),
    ],
    ids=["add_new_option", "replace_option", "replace_non_existing_option"],
)
def test_add_or_replace_command_line_option(command_line, option, replace_option, expected_command_line):
    actual_command_line = add_or_replace_command_line_option(command_line, option, replace_option)
    assert actual_command_line == expected_command_line


@pytest.mark.parametrize(
    "command_line, command_option, expected_command_line",
    [
        (
            ["dbt", "--foo", "run", "--select", "orders"],
            "--foo",
            ["dbt", "run", "--select", "orders"],
        ),
        (
            ["dbt", "run", "--select", "orders"],
            "--bar",
            ["dbt", "run", "--select", "orders"],
        ),
        (
            ["dbt", CONSUME_STRUCTURED_LOGS_COMMAND_OPTION, "run", "--select", "orders"],
            CONSUME_STRUCTURED_LOGS_COMMAND_OPTION,
            ["dbt", "run", "--select", "orders"],
        ),
    ],
    ids=[
        "remove_existing_command_option",
        "remove_absent_command_option",
        "remove_consume_structured_logs_command_option",
    ],
)
def test_remove_command_line_option(command_line, command_option, expected_command_line):
    actual_command_line = remove_command_line_option(command_line, command_option)
    assert actual_command_line == expected_command_line


@pytest.mark.parametrize(
    "incremental_reads, expected_lines",
    [
        (["foo", " bar", " bim\nbuzz", " foo\n"], ["foo bar bim", "buzz foo"]),
        (["foo bar bim\n", "buzz foo\n"], ["foo bar bim", "buzz foo"]),
        (["foo bar bim\n", "buzz foo"], ["foo bar bim"]),
        (["foo", " bar", " bim\nbuzz\nfizz\n", "foo\n"], ["foo bar bim", "buzz", "fizz", "foo"]),
    ],
    ids=[
        "new_line_in_middle",
        "new_line_at_the_end",
        "last_read_has_no_new_line",
        "many_new_lines_in_one_read",
    ],
)
def test_incremental_file_reader(incremental_reads, expected_lines):
    class DummyTextFile:
        """
        This simulates a text file that reads incomplete lines.
        """

        def __init__(self):
            self.incremental_reads = incremental_reads
            self.name = "dummy"
            self.i = 0

        def read(self, *args, **kwargs):
            """
            This simulates incomplete reads.
            """
            next_read = self.incremental_reads[self.i] if self.i < len(incremental_reads) else ""
            self.i += 1
            return next_read

    dummy_text_file = DummyTextFile()
    incremental_reader = IncrementalFileReader(dummy_text_file)
    actual_lines_read = []
    for line in incremental_reader.read_lines(4096):
        actual_lines_read.append(line)

    assert expected_lines == actual_lines_read
