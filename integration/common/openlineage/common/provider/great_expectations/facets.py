# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Union, Dict

import attr
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchMarkers, BatchDefinition
from great_expectations.core.id_dict import BatchSpec, BatchKwargs
from openlineage.client.facet import BaseFacet

from openlineage.common.provider.great_expectations.results import GreatExpectationsAssertion


@attr.s
class GreatExpectationsRunFacet(BaseFacet):
    """
    Custom facet which describes the instance of GreatExpectations and the suite configuration
    """
    great_expectations_version = attr.ib()
    expectation_suite_name: str = attr.ib()
    run_id: Dict = attr.ib(converter=lambda x: x.to_json_dict())    # type: ignore
    expectation_suite_meta: Dict = attr.ib()
    validation_time: str = attr.ib()
    batch_spec: Optional[BatchSpec] = attr.ib(default=None)
    batch_markers: Optional[BatchMarkers] = attr.ib(default=None)
    batch_kwargs: Optional[BatchKwargs] = attr.ib(default=None)
    active_batch_definition: Union[None, IDDict, BatchDefinition] = attr.ib(default=None)
    batch_parameters = attr.ib(default=None)

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-run-facet.json"  # noqa


@attr.s
class GreatExpectationsAssertionsDatasetFacet(BaseFacet):
    """
    This facet represents passed/failed status of asserted expectations on dataset
    """
    assertions: List[GreatExpectationsAssertion] = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/OpenLineage/OpenLineage/tree/main/integration/common/openlineage/common/provider/ge-assertions-dataset-facet.json"  # noqa
