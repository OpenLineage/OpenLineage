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
from enum import Enum
from typing import List, Union, Type, Optional, Dict
from abc import ABC, abstractmethod

from airflow import LoggingMixin
from airflow.models import BaseOperator

from marquez_airflow.models import DbTableSchema, DbColumn
from openlineage.facet import BaseFacet


class DatasetType(Enum):
    DB_TABLE = "DB_TABLE"
    STREAM = "STREAM"


class Source:
    name = None
    connection_url = None
    type = None

    def __init__(self, name, type, connection_url):
        self.name = name
        self.type = type
        self.connection_url = connection_url

    def __eq__(self, other):
        return self.name == other.name and \
               self.type == other.type and \
               self.connection_url == other.connection_url

    def __repr__(self):
        return f"Source({self.name!r},{self.type!r},{self.connection_url!r})"


class Field:
    def __init__(self, name: str, type: str,
                 tags: List[str] = None, description: str = None):
        self.name = name
        self.type = type
        self.tags = tags
        self.description = description

        if self.tags is None:
            self.tags = []

    @staticmethod
    def from_column(column: DbColumn):
        return Field(
            name=column.name,
            type=column.type,
            description=column.description
        )

    def __eq__(self, other):
        return self.name == other.name and \
               self.type == other.type and \
               self.tags == other.tags and \
               self.description == other.description

    def __repr__(self):
        return f"Field({self.name!r},{self.type!r}, \
                       {self.tags!r},{self.description!r})"


class Dataset:
    def __init__(self, source: Source, name: str, type: DatasetType,
                 fields: List[Field] = None, description: Optional[str] = None,
                 custom_facets: Dict[str, Type[BaseFacet]] = None):
        if fields is None:
            fields = []
        if custom_facets is None:
            custom_facets = {}
        self.source = source
        self.name = name
        self.type = type
        self.fields = fields
        self.description = description
        self.custom_facets = custom_facets

    @staticmethod
    def from_table(source: Source, table_name: str,
                   schema_name: str = None):
        return Dataset(
            type=DatasetType.DB_TABLE,
            name=Dataset._to_name(
                schema_name=schema_name,
                table_name=table_name
            ),
            source=source
        )

    @staticmethod
    def from_table_schema(source: Source, table_schema: DbTableSchema):
        return Dataset(
            type=DatasetType.DB_TABLE,
            name=Dataset._to_name(
                schema_name=table_schema.schema_name,
                table_name=table_schema.table_name.name
            ),
            source=source,
            fields=[
                # We want to maintain column order using ordinal position.
                Field.from_column(column) for column in sorted(
                    table_schema.columns, key=lambda x: x.ordinal_position
                )
            ]
        )

    @staticmethod
    def _to_name(table_name: str, schema_name: str = None):
        # Prefix the table name with the schema name using
        # the format: {table_schema}.{table_name}.
        return f"{schema_name}.{table_name}" if schema_name else table_name

    def __eq__(self, other):
        return self.source == other.source and \
               self.name == other.name and \
               self.type == other.type and \
               self.fields == other.fields and \
               self.description == other.description

    def __repr__(self):
        return f"Dataset({self.source!r},{self.name!r}, \
                         {self.type!r},{self.fields!r},{self.description!r})"


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

    @classmethod
    def get_operator_class(cls):
        return cls.operator_class

    def validate(self):
        # TODO: maybe we should also enforce the module
        assert (self.operator_class is not None and
                self.operator.__class__ == self.operator_class)

    @abstractmethod
    def extract(self) -> Union[StepMetadata, List[StepMetadata]]:
        # In future releases, we'll want to deprecate returning a list of StepMetadata
        # and simply return a StepMetadata object. We currently return a list
        # for backwards compatibility.
        pass

    def extract_on_complete(self, task_instance) -> Union[StepMetadata, List[StepMetadata]]:
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
