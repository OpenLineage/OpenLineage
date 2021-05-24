# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools
import logging
from collections import defaultdict

import attr
from typing import Optional, Any, Dict

from marquez_airflow.facets import DataQualityDatasetFacet, ColumnMetric
from marquez_airflow.utils import get_from_nullable_chain, get_job_name

from marquez_airflow.extractors import (
    BaseExtractor,
    StepMetadata, Dataset, Source, DatasetType,
)


def wrap_callback(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        result = f(self, *args, **kwargs)
        self._extractor.store_result(result)
        return result

    return wrapper


log = logging.getLogger(__file__)


# Great Expectations is optional dependency.
try:
    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
    _has_great_expectations = True
    GreatExpectationsOperator.execute = wrap_callback(GreatExpectationsOperator.execute)
except (ImportError, ModuleNotFoundError):
    # Create placeholder for GreatExpectationsOperator
    GreatExpectationsOperator = None
    log.info('Did not find great_expectations_provider library')
    _has_great_expectations = False


@attr.s
class ExpectationsParserResult:
    """
    Internal class to represent actual expectation values, per table and optionally per column
    """
    facet_key: str = attr.ib()
    value: Any = attr.ib()
    column_id: Optional[str] = attr.ib(default=None)


class GreatExpectationsExtractorImpl(BaseExtractor):
    """
    Great Expectations extractor extracts validation data from CheckpointResult object and
    parses it via ExpectationsParsers. Results are used to prepare data quality facet.
    """
    operator_class = GreatExpectationsOperator

    def __init__(self, operator):
        super().__init__(operator)
        self.operator._extractor = self
        self.result = None

    def store_result(self, result):
        self.result = result

    def parse_result(self) -> Optional[Dataset]:
        try:
            run_result = self.result['run_results']
            # Great expectations generate long, impenetrable name here
            validation_result = next(iter(run_result.values()))['validation_result']

            data_quality_facet = self.parse_data_quality_facet(validation_result)
            if not data_quality_facet:
                return None

            batch_kwargs = get_from_nullable_chain(
                validation_result,
                ['meta', 'batch_kwargs']
            )
            # To match dataset name we need canonical datasource name
            name = batch_kwargs.get('datasource', None)
            path = batch_kwargs.get('path', None)
            return Dataset(
                source=Source(
                    name=name,
                    type='GREAT_EXPECTATIONS',
                    connection_url=path
                ),
                name=name,
                type=DatasetType.DB_TABLE,
                custom_facets={
                    'dataQuality': data_quality_facet
                }
            )

        except ValueError:
            pass
        return None

    def parse_data_quality_facet(self, validation_result: Dict) \
            -> Optional[DataQualityDatasetFacet]:
        facet_data = {
            "columnMetrics": defaultdict(dict)
        }

        # try to get to actual expectations results
        try:
            expectations_results = validation_result['results']
            for expectation in expectations_results:
                for parser in _EXPECTATIONS_PARSERS:

                    # accept possible duplication, should have no difference in results
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data[result.facet_key] = result.value
                for parser in _COLUMN_EXPECTATIONS_PARSER:
                    if parser.can_accept(expectation):
                        result = parser.parse_expectation_result(expectation)
                        facet_data['columnMetrics'][result.column_id][result.facet_key] \
                            = result.value

            for key in facet_data['columnMetrics'].keys():
                facet_data['columnMetrics'][key] = ColumnMetric(**facet_data['columnMetrics'][key])
            return DataQualityDatasetFacet(**facet_data)
        except ValueError:
            log.exception(
                "Great Expectations's CheckpointResult object does not have expected key"
            )
        return None

    def extract(self) -> Optional[StepMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[StepMetadata]:
        if self.result:
            dataset = self.parse_result()
            if not dataset:
                return None
            return StepMetadata(
                name=get_job_name(task=self.operator),
                inputs=[dataset]
            )
        return None


class ExpectationsParser:
    """
    Base expectation parser. Dispatches parser looking at expectation type.
    Implementations should extract result from result dictionary.
    """
    expectation_key: str = ''
    facet_key: str = ''

    @classmethod
    def can_accept(cls, expectation_results: dict) -> bool:
        expectation_type = get_from_nullable_chain(
            expectation_results,
            ['expectation_config', 'expectation_type']
        )
        return expectation_type and expectation_type == cls.expectation_key

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:
        raise NotImplementedError("")


class BetweenRowCountExpectationsParser(ExpectationsParser):
    expectation_key = 'expect_table_row_count_to_be_between'
    facet_key = 'rowCount'

    @classmethod
    def parse_expectation_result(cls, expectation_result: dict) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            cls.facet_key,
            get_from_nullable_chain(
                expectation_result,
                ['result', 'observed_value']
            )
        )


class EqualRowCountExpectationsParser(BetweenRowCountExpectationsParser):
    expectation_key = 'expect_table_row_count_to_equal'


class FileSizeExpectationsParser(ExpectationsParser):
    expectation_key = 'expect_file_size_to_be_between'

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:
        pass  # TODO: file asset validation


class ColumnExpectationsParser(ExpectationsParser):
    """
    Extractor for column-based expectations. Looks at column name in addition to expectation type
    """
    column = ''

    @classmethod
    def can_accept(cls, expectation_results: dict) -> bool:
        expectation_type = get_from_nullable_chain(
            expectation_results,
            ['expectation_config', 'expectation_type']
        )
        extracted_column = get_from_nullable_chain(
            expectation_results,
            ['expectation_config', 'kwargs', 'column']
        )
        return expectation_type and extracted_column \
            and expectation_type == cls.expectation_key


class ValuesNotNullColumnExpectationParser(ColumnExpectationsParser):
    expectation_key = 'expect_column_values_to_not_be_null'

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> Any:
        return ExpectationsParserResult(
            'nullCount',
            get_from_nullable_chain(expectation_result, ['result', 'unexpected_count']),
            get_from_nullable_chain(
                expectation_result,
                ['expectation_config', 'kwargs', 'column']
            )
        )


class ValuesDistinctExpectationParser(ColumnExpectationsParser):
    expectation_key = 'expect_column_unique_value_count_to_be_between'

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            'distinctCount',
            get_from_nullable_chain(expectation_result, ['result', 'observed_value']),
            get_from_nullable_chain(
                expectation_result,
                ['expectation_config', 'kwargs', 'column']
            )
        )


class ValuesAverageExpectationParser(ColumnExpectationsParser):
    expectation_key = 'expect_column_sum_to_be_between'

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:
        sum = get_from_nullable_chain(expectation_result, ['result', 'observed_value'])
        count = get_from_nullable_chain(expectation_result, ['result', 'element_count'])
        return ExpectationsParserResult(
            'average',
            sum / count,
            get_from_nullable_chain(
                expectation_result,
                ['expectation_config', 'kwargs', 'column']
            )
        )


class ValuesMaxExpectationParser(ColumnExpectationsParser):
    expectation_key = 'expect_column_max_to_be_between'

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            'max',
            get_from_nullable_chain(expectation_result, ['result', 'observed_value']),
            get_from_nullable_chain(
                expectation_result,
                ['expectation_config', 'kwargs', 'column']
            )
        )


class ValuesMinExpectationParser(ColumnExpectationsParser):
    expectation_key = 'expect_column_min_to_be_between'

    @staticmethod
    def parse_expectation_result(expectation_result: dict) -> ExpectationsParserResult:
        return ExpectationsParserResult(
            'min',
            get_from_nullable_chain(expectation_result, ['result', 'observed_value']),
            get_from_nullable_chain(
                expectation_result,
                ['expectation_config', 'kwargs', 'column']
            )
        )


class ValuesQuantileExpectationParser(ColumnExpectationsParser):
    expectation_key = 'expect_column_quantile_values_to_be_between'

    @staticmethod
    def quantile_to_map(observations):
        return {
            str(k): v for k, v in zip(observations['quantiles'], observations['values'])
        }

    @classmethod
    def parse_expectation_result(cls, expectation_result: dict) -> ExpectationsParserResult:
        observed_values = get_from_nullable_chain(expectation_result, ['result', 'observed_value'])
        return ExpectationsParserResult(
            'quantiles',
            cls.quantile_to_map(
                observed_values
            ) if observed_values else None,
            get_from_nullable_chain(
                expectation_result,
                ['expectation_config', 'kwargs', 'column']
            )
        )


_EXPECTATIONS_PARSERS = [
    BetweenRowCountExpectationsParser,
    EqualRowCountExpectationsParser,
    # FileSizeExpectationsParser,
]

_COLUMN_EXPECTATIONS_PARSER = [
    ValuesNotNullColumnExpectationParser,
    ValuesDistinctExpectationParser,
    ValuesMinExpectationParser,
    ValuesMaxExpectationParser,
    ValuesQuantileExpectationParser,
    ValuesAverageExpectationParser
]

if _has_great_expectations:
    GreatExpectationsExtractor = GreatExpectationsExtractorImpl
else:
    class GreatExpectationsExtractor:
        def __init__(self):
            raise RuntimeError('Great Expectations provider not found')
