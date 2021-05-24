"""
Unittest module to test Great Expectations Operators.

Requires the unittest, pytest, and requests-mock Python libraries.
pip install google-cloud-storage

Run test:

    python3 -m unittest test.operators.test_operators.TestGreatExpectationsOperator

"""
from pathlib import Path
import logging
import os

from marquez_airflow.extractors.great_expectations_extractor import GreatExpectationsExtractor

from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

from marquez_airflow.facets import DataQualityDatasetFacet, ColumnMetric
from openlineage.run import Serde

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

    log.info(result)

    expected_dq = DataQualityDatasetFacet(
        rowCount=10000,
        columnMetrics={
            'vendor_id': ColumnMetric(
                nullCount=0,
                distinctCount=3
            ),
            'total_amount': ColumnMetric(
                average=15.724231000000003,
                min=-52.8,
                max=3004.8,
                quantiles={"0": -52.8, "0.333": 9.3, "0.6667": 14.16, "1": 3004.8}
            )
        }
    )

    assert Serde.to_json(step.inputs[0].custom_facets['dataQuality']) == \
           Serde.to_json(expected_dq)
    assert result['success']
