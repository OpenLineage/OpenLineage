from abc import ABC, abstractmethod
from typing import List, Dict, Type, Union, Optional

from openlineage.client.run import Dataset
from pkg_resources import parse_version

from airflow.models import BaseOperator
from airflow.version import version as AIRFLOW_VERSION

from openlineage.client.facet import BaseFacet

if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin
elif parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin


class StepMetadata:
    def __init__(
            self,
            name,
            location=None,
            inputs: List[Dataset] = None,
            outputs: List[Dataset] = None,
            context=None,
            run_facets: Dict[str, BaseFacet] = None
    ):
        # TODO: Define a common way across extractors to build the
        # job name for an operator
        self.name = name
        self.location = location
        self.inputs = inputs
        self.outputs = outputs
        self.context = context
        self.run_facets = run_facets

        if not inputs:
            self.inputs = []
        if not outputs:
            self.outputs = []
        if not context:
            self.context = {}
        if not run_facets:
            self.run_facets = {}

    def __repr__(self):
        return "name: {}\t inputs: {} \t outputs: {}".format(
            self.name,
            ','.join([str(i) for i in self.inputs]),
            ','.join([str(o) for o in self.outputs]))


class BaseExtractor(ABC, LoggingMixin):
    operator_class: Type[BaseOperator] = None
    operator: operator_class = None

    def __init__(self, operator):
        self.operator = operator
        self.patch()

    def patch(self):
        # Extractor should register extension methods or patches to operator here
        pass

    @classmethod
    def get_operator_class(cls):
        return cls.operator_class

    def validate(self):
        # TODO: maybe we should also enforce the module
        assert (self.operator_class is not None and
                self.operator.__class__ == self.operator_class)

    @abstractmethod
    def extract(self) -> Union[Optional[StepMetadata], List[StepMetadata]]:
        # In future releases, we'll want to deprecate returning a list of StepMetadata
        # and simply return a StepMetadata object. We currently return a list
        # for backwards compatibility.
        pass

    def extract_on_complete(self, task_instance) -> \
            Union[Optional[StepMetadata], List[StepMetadata]]:
        return self.extract()
