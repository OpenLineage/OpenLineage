from abc import ABC, abstractmethod
from typing import List, Dict, Type, Union, Optional

from airflow import LoggingMixin
from airflow.models import BaseOperator
from openlineage.facet import BaseFacet

from marquez.dataset import Dataset


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
        # TODO: This method allows for the partial updating of task
        # metadata on completion. Marquez currently doesn't support
        # partial updates within the context of a DAG run, but this feature
        # will soon be supported:
        # https://github.com/MarquezProject/marquez/issues/816
        #
        # Also, we'll want to revisit the metadata extraction flow,
        # but for now, return an empty set as the default behavior
        # as not all extractors need to handle partial metadata updates.
        return self.extract()
