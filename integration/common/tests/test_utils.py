# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.common.utils import (
    get_from_nullable_chain,
    parse_multiple_args,
    parse_single_arg,
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
