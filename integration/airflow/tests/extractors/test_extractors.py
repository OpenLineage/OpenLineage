import os


from openlineage.airflow.extractors import Extractors
from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor


def test_basic_extractor():
    class PostgresOperator:
        pass

    assert Extractors().get_extractor_class(PostgresOperator)


def test_env_extractors():
    os.environ['OPENLINEAGE_EXTRACTOR_TestOperator'] = \
        'openlineage.airflow.extractors.postgres_extractor.PostgresExtractor'

    assert len(Extractors().extractors) == 6
    del os.environ['OPENLINEAGE_EXTRACTOR_TestOperator']


def test_adding_extractors():
    extractors = Extractors()
    count = len(extractors.extractors)
    extractors.add_extractor("test", PostgresExtractor)
    assert len(extractors.extractors) == count + 1
