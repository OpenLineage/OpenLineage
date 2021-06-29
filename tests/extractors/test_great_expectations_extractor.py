"""
Unittest module to test Great Expectations Operators.

Requires the unittest, pytest, and requests-mock Python libraries.
pip install google-cloud-storage

Run test:

    python3 -m unittest test.operators.test_operators.TestGreatExpectationsOperator

"""
import logging
import os
import pytest
from pathlib import Path

from marquez_airflow.extractors.great_expectations_extractor import \
    GreatExpectationsAssertionsDatasetFacet, GreatExpectationsAssertion, \
    GreatExpectationsExtractor, set_dataset_info

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from marquez_airflow.facets import DataQualityDatasetFacet, ColumnMetric
from openlineage.serde import Serde

log = logging.getLogger(__name__)

# Set relative paths for Great Expectations directory and sample data
base_path = Path(__file__).parents[1]
data_file = os.path.join(base_path,
                         'data',
                         'great_expectations',
                         'data',
                         'yellow_tripdata_sample_2019-01.csv')
ge_root_dir = os.path.join(base_path, 'data', 'great_expectations', 'great_expectations')


def test_great_expectations_operator_batch_kwargs_success():
    operator = GreatExpectationsOperator(
        task_id='ge_batch_kwargs_pass',
        expectation_suite_name='taxi.demo',
        batch_kwargs={
            'path': data_file,
            'datasource': 'data__dir'
        },
        data_context_root_dir=ge_root_dir,
        fail_task_on_validation_failure=False
    )

    extractor = GreatExpectationsExtractor(operator)
    result = operator.execute({})
    step = extractor.extract_on_complete(None)

    expected_dq = DataQualityDatasetFacet(
        rowCount=10000,
        columnMetrics={
            'vendor_id': ColumnMetric(
                nullCount=0,
                distinctCount=3
            ),
            'total_amount': ColumnMetric(
                sum=157242.31000000003,
                count=10000,
                min=-52.8,
                max=3004.8,
                quantiles={"0": -52.8, "0.333": 9.3, "0.6667": 14.16, "1": 3004.8}
            )
        }
    )

    expected_assertions = GreatExpectationsAssertionsDatasetFacet(
        assertions=[
            GreatExpectationsAssertion(
                expectationType='expect_table_row_count_to_be_between',
                success=True
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_values_to_not_be_null',
                success=True,
                columnId='vendor_id'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_distinct_values_to_be_in_set',
                success=True,
                columnId='vendor_id'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_unique_value_count_to_be_between',
                success=True,
                columnId='vendor_id'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_min_to_be_between',
                success=True,
                columnId='total_amount'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_max_to_be_between',
                success=True,
                columnId='total_amount'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_sum_to_be_between',
                success=True,
                columnId='total_amount'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_quantile_values_to_be_between',
                success=True,
                columnId='total_amount'
            ),
            GreatExpectationsAssertion(
                expectationType='expect_column_distinct_values_to_be_in_set',
                success=True,
                columnId='passenger_count'
            )
        ]
    )

    assert Serde.to_json(step.inputs[0].custom_facets['dataQuality']) == \
           Serde.to_json(expected_dq)
    assert step.inputs[0].custom_facets['greatExpectations_assertions'] == expected_assertions

    assert result['success']


@pytest.mark.parametrize('namespace,scheme,authority', [
    ('postgres://localhost:5432', 'postgres', 'localhost:5432'),
    ('bigquery:', 'bigquery', ''),
    ('s3://bucket', "s3", 'bucket')
])
def test_great_expectations_operator_with_provided_namespace_success(namespace, scheme, authority):
    operator = GreatExpectationsOperator(
        task_id='ge_batch_kwargs_pass',
        expectation_suite_name='taxi.demo',
        batch_kwargs={
            'path': data_file,
            'datasource': 'data__dir'
        },
        data_context_root_dir=ge_root_dir,
        fail_task_on_validation_failure=False
    )

    set_dataset_info(operator, namespace, 'schema.table')

    extractor = GreatExpectationsExtractor(operator)
    result = operator.execute({})
    step = extractor.extract_on_complete(None)

    expected_dq = DataQualityDatasetFacet(
        rowCount=10000,
        columnMetrics={
            'vendor_id': ColumnMetric(
                nullCount=0,
                distinctCount=3
            ),
            'total_amount': ColumnMetric(
                sum=157242.31000000003,
                count=10000,
                min=-52.8,
                max=3004.8,
                quantiles={"0": -52.8, "0.333": 9.3, "0.6667": 14.16, "1": 3004.8}
            )
        }
    )

    assert Serde.to_json(step.inputs[0].custom_facets['dataQuality']) == \
           Serde.to_json(expected_dq)
    assert step.inputs[0].source.scheme == scheme
    assert step.inputs[0].source.authority == authority
    assert step.inputs[0].name == 'schema.table'
    assert result['success']
