# SPDX-License-Identifier: Apache-2.0

import os
from typing import List, Optional
from unittest.mock import patch

from openlineage.airflow.extractors import Extractors, BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.postgres_extractor import PostgresExtractor


class FakeExtractor(BaseExtractor):
    def extract(self) -> Optional[TaskMetadata]:
        return None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['TestOperator']


class AnotherFakeExtractor(BaseExtractor):
    def extract(self) -> Optional[TaskMetadata]:
        return None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['AnotherTestOperator']


def test_basic_extractor():
    class PostgresOperator:
        pass

    assert Extractors().get_extractor_class(PostgresOperator)


def test_env_add_extractor():
    extractor_list_len = len(Extractors().extractors)
    with patch.dict(os.environ, {"OPENLINEAGE_EXTRACTORS": "tests.extractors.test_extractors.FakeExtractor"}):  # noqa
        assert len(Extractors().extractors) == extractor_list_len + 1


def test_env_multiple_extractors():
    extractor_list_len = len(Extractors().extractors)
    with patch.dict(os.environ, {"OPENLINEAGE_EXTRACTORS": "tests.extractors.test_extractors.FakeExtractor;tests.extractors.test_extractors.AnotherFakeExtractor"}):  # noqa
        assert len(Extractors().extractors) == extractor_list_len + 2


def test_env_old_method_extractors():
    extractor_list_len = len(Extractors().extractors)

    os.environ['OPENLINEAGE_EXTRACTOR_TestOperator'] = \
        'tests.extractors.test_extractors.FakeExtractor'

    assert len(Extractors().extractors) == extractor_list_len + 1
    del os.environ['OPENLINEAGE_EXTRACTOR_TestOperator']


def test_adding_extractors():
    extractors = Extractors()
    count = len(extractors.extractors)
    extractors.add_extractor("test", PostgresExtractor)
    assert len(extractors.extractors) == count + 1
