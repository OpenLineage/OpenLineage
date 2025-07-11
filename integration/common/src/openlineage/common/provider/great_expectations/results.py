# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Optional

import attr
from openlineage.common.utils import get_from_nullable_chain

from great_expectations.core import ExpectationValidationResult


@attr.define
class ExpectationsParserResult:
    """
    Internal class to represent actual expectation values, per table and optionally per column
    """

    facet_key: str
    value: Any
    column_id: Optional[str] = None


@attr.define
class GreatExpectationsAssertion:
    expectationType: str
    success: bool
    column: Optional[str] = None


class ExpectationsParser:
    """
    Base expectation parser. Dispatches parser looking at expectation type.
    Implementations should extract result from result dictionary.
    """

    expectation_key: str = ""
    facet_key: str = ""

    @classmethod
    def can_accept(cls, expectation_result: ExpectationValidationResult) -> Optional[Any]:
        expectation_type = get_from_nullable_chain(
            expectation_result, ["expectation_config", "expectation_type"]
        )
        return expectation_type and expectation_type == cls.expectation_key

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> ExpectationsParserResult:
        raise NotImplementedError("")


class BetweenRowCountExpectationsParser(ExpectationsParser):
    expectation_key = "expect_table_row_count_to_be_between"
    facet_key = "rowCount"

    @classmethod
    def parse_expectation_result(
        cls, expectation_result: ExpectationValidationResult
    ) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            cls.facet_key,
            get_from_nullable_chain(expectation_result, ["result", "observed_value"]),
        )


class EqualRowCountExpectationsParser(BetweenRowCountExpectationsParser):
    expectation_key = "expect_table_row_count_to_equal"


class FileSizeExpectationsParser(ExpectationsParser):
    expectation_key = "expect_file_size_to_be_between"

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:  # type: ignore # noqa
        pass  # TODO: file asset validation


class ColumnExpectationsParser(ExpectationsParser):
    """
    Extractor for column-based expectations. Looks at column name in addition to expectation type
    """

    column = ""

    @classmethod
    def can_accept(cls, expectation_result: ExpectationValidationResult) -> Optional[Any]:
        expectation_type = get_from_nullable_chain(
            expectation_result, ["expectation_config", "expectation_type"]
        )
        extracted_column = get_from_nullable_chain(
            expectation_result, ["expectation_config", "kwargs", "column"]
        )
        return expectation_type and extracted_column and expectation_type == cls.expectation_key


class ValuesNotNullColumnExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_values_to_not_be_null"

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> Any:
        return ExpectationsParserResult(
            "nullCount",
            get_from_nullable_chain(expectation_result, ["result", "unexpected_count"]),
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


class ValuesDistinctExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_unique_value_count_to_be_between"

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            "distinctCount",
            get_from_nullable_chain(expectation_result, ["result", "observed_value"]),
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


class ValuesSumExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_sum_to_be_between"

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> ExpectationsParserResult:
        sum = get_from_nullable_chain(expectation_result, ["result", "observed_value"])
        return ExpectationsParserResult(
            "sum",
            sum,
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


class ValuesCountExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_sum_to_be_between"

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> ExpectationsParserResult:
        count = get_from_nullable_chain(expectation_result, ["result", "element_count"])
        return ExpectationsParserResult(
            "count",
            count,
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


class ValuesMaxExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_max_to_be_between"

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            "max",
            get_from_nullable_chain(expectation_result, ["result", "observed_value"]),
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


class ValuesMinExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_min_to_be_between"

    @staticmethod
    def parse_expectation_result(expectation_result: ExpectationValidationResult) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            "min",
            get_from_nullable_chain(expectation_result, ["result", "observed_value"]),
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


class ValuesQuantileExpectationParser(ColumnExpectationsParser):
    expectation_key = "expect_column_quantile_values_to_be_between"

    @staticmethod
    def quantile_to_map(observations):
        return {str(k): v for k, v in zip(observations["quantiles"], observations["values"])}

    @classmethod
    def parse_expectation_result(cls, expectation_result: dict) -> ExpectationsParserResult:
        observed_values = get_from_nullable_chain(expectation_result, ["result", "observed_value"])
        return ExpectationsParserResult(
            "quantiles",
            cls.quantile_to_map(observed_values) if observed_values else None,
            get_from_nullable_chain(expectation_result, ["expectation_config", "kwargs", "column"]),
        )


EXPECTATIONS_PARSERS = [
    BetweenRowCountExpectationsParser,
    EqualRowCountExpectationsParser,
    # FileSizeExpectationsParser,
]

COLUMN_EXPECTATIONS_PARSER = [
    ValuesNotNullColumnExpectationParser,
    ValuesDistinctExpectationParser,
    ValuesMinExpectationParser,
    ValuesMaxExpectationParser,
    ValuesQuantileExpectationParser,
    ValuesSumExpectationParser,
    ValuesCountExpectationParser,
]
