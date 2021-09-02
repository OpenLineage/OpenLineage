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
import logging
from typing import Optional

from openlineage.airflow.extractors.base import BaseExtractor, StepMetadata

log = logging.getLogger(__file__)


# Great Expectations is optional dependency.
try:
    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
    _has_great_expectations = True
except Exception:
    # Create placeholder for GreatExpectationsOperator
    GreatExpectationsOperator = None
    log.warning('Did not find great_expectations_provider library or failed to import it')
    _has_great_expectations = False


class GreatExpectationsExtractorImpl(BaseExtractor):
    """
    Great Expectations extractor extracts validation data from CheckpointResult object and
    parses it via ExpectationsParsers. Results are used to prepare data quality facet.
    """
    operator_class = GreatExpectationsOperator

    def __init__(self, operator):
        super().__init__(operator)
        self.result = None

    def extract(self) -> Optional[StepMetadata]:
        return None

    def extract_on_complete(self, task_instance) -> Optional[StepMetadata]:
        return None


if _has_great_expectations:
    GreatExpectationsExtractor = GreatExpectationsExtractorImpl
else:
    class GreatExpectationsExtractor:
        def __init__(self):
            raise RuntimeError('Great Expectations provider not found')
