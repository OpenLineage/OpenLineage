# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata

# Great Expectations is optional dependency.
try:
    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
    _has_great_expectations = True
except Exception:
    # Create placeholder for GreatExpectationsOperator
    GreatExpectationsOperator = None
    _has_great_expectations = False


class GreatExpectationsExtractorImpl(BaseExtractor):
    """
    Great Expectations extractor extracts validation data from CheckpointResult object and
    parses it via ExpectationsParsers. Results are used to prepare data quality facet.
    """
    def __init__(self, operator):
        super().__init__(operator)
        self.result = None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return [GreatExpectationsOperator.__name__] if GreatExpectationsOperator else []

    def extract(self) -> Optional[TaskMetadata]:
        return None


if _has_great_expectations:
    GreatExpectationsExtractor = GreatExpectationsExtractorImpl
else:
    class GreatExpectationsExtractor:   # type: ignore
        def __init__(self):
            raise RuntimeError('Great Expectations provider not found')

        @classmethod
        def get_operator_classnames(cls) -> List[str]:
            return []
