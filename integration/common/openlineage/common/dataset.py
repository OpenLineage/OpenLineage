# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import ClassVar, Dict, List, Optional
from urllib.parse import urlparse

from openlineage.client.event_v2 import Dataset as OpenLineageDataset
from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.facet_v2 import (
    BaseFacet,
    DatasetFacet,
    datasource_dataset,
    documentation_dataset,
    schema_dataset,
    symlinks_dataset,
)
from openlineage.client.utils import RedactMixin
from openlineage.common.models import DbColumn, DbTableSchema


class Source(RedactMixin):
    _skip_redact: ClassVar[List[str]] = ["scheme", "name"]

    def __init__(
        self,
        scheme: Optional[str] = None,
        authority: Optional[str] = None,
        connection_url: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.scheme = scheme
        self.authority = authority
        self._name = name
        self.connection_url = connection_url

        if (scheme or authority) and name:
            raise RuntimeError("scheme + authority and namespace are exclusive options")

    def __eq__(self, other):
        return self.name == other.name and self.connection_url == other.connection_url

    def __repr__(self):
        authority = "//" + self.authority if self.authority else ""
        return f"Source({self.scheme!r}:{authority} - {self.connection_url!r})"

    @property
    def name(self) -> str:
        if self._name:
            return self._name
        if self.authority:
            return f"{self.scheme}://{self.authority}"
        return f"{self.scheme}"


class Field(RedactMixin):
    _skip_redact: ClassVar[List[str]] = ["name", "type", "tags"]

    def __init__(
        self,
        name: str,
        type: str,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
    ):
        self.name = name
        self.type = type
        self.tags = tags
        self.description = description

        if self.tags is None:
            self.tags = []

    @staticmethod
    def from_column(column: DbColumn):
        return Field(name=column.name, type=column.type, description=column.description)

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.type == other.type
            and self.tags == other.tags
            and self.description == other.description
        )

    def __repr__(self):
        return f"Field({self.name!r},{self.type!r}, \
                       {self.tags!r},{self.description!r})"


class Dataset(RedactMixin):
    _skip_redact: ClassVar[List[str]] = ["name"]

    def __init__(
        self,
        source: Source,
        name: str,
        fields: Optional[List[Field]] = None,
        description: Optional[str] = None,
        custom_facets: Optional[Dict[str, BaseFacet]] = None,
        input_facets: Optional[Dict[str, BaseFacet]] = None,
        output_facets: Optional[Dict[str, BaseFacet]] = None,
    ):
        if fields is None:
            fields = []
        if custom_facets is None:
            custom_facets = {}
        if input_facets is None:
            input_facets = {}
        if output_facets is None:
            output_facets = {}
        self.source = source
        self.name = name
        self.fields = fields
        self.description = description
        self.custom_facets = custom_facets
        self.input_facets = input_facets
        self.output_facets = output_facets

    @staticmethod
    def from_table(
        source: Source,
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None,
    ):
        return Dataset(
            name=Dataset._to_name(
                schema_name=schema_name,
                table_name=table_name,
                database_name=database_name,
            ),
            source=source,
        )

    @staticmethod
    def from_table_schema(
        source: Source,
        table_schema: DbTableSchema,
        database_name: Optional[str] = None,
        data_location: Optional[str] = None,
    ):
        parsed_path = urlparse(data_location)
        custom_facets = (
            {
                "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                    identifiers=[
                        symlinks_dataset.Identifier(
                            namespace=f"{parsed_path.scheme}://{parsed_path.netloc}",  # type: ignore
                            name=str(parsed_path.path),
                            type="TABLE",
                        )
                    ]
                )
            }
            if data_location is not None
            else None
        )

        return Dataset(
            name=Dataset._to_name(
                schema_name=table_schema.schema_name,
                table_name=table_schema.table_name.name,
                database_name=database_name,
            ),
            source=source,
            fields=[
                # We want to maintain column order using ordinal position.
                Field.from_column(column)
                for column in sorted(table_schema.columns, key=lambda x: x.ordinal_position)
            ],
            custom_facets=custom_facets,  # type: ignore
        )

    @staticmethod
    def _to_name(
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None,
    ):
        # Prefix the table name with database and schema name using
        # the format: {database_name}.{table_schema}.{table_name}.
        name = [table_name]
        if schema_name is not None:
            name = [schema_name] + name
        if database_name is not None:
            name = [database_name] + name

        return ".".join(name)

    def __eq__(self, other):
        return (
            self.source == other.source
            and self.name == other.name
            and self.fields == other.fields
            and self.description == other.description
        )

    def __repr__(self):
        return f"Dataset({self.source!r},{self.name!r}, \
                         {self.fields!r},{self.description!r})"

    def to_openlineage_dataset(self) -> OpenLineageDataset:
        facets: Dict[str, DatasetFacet] = {
            "dataSource": datasource_dataset.DatasourceDatasetFacet(
                name=self.source.name,
                uri=self.source.connection_url or "",
            )
        }
        if self.description:
            facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
                description=self.description
            )

        if self.fields is not None and len(self.fields):
            facets["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[
                    schema_dataset.SchemaDatasetFacetFields(field.name, field.type, field.description)
                    for field in self.fields
                ]
            )

        if self.custom_facets:
            facets.update(self.custom_facets)  # type: ignore

        if len(self.input_facets):
            return InputDataset(
                namespace=self.source.name,
                name=self.name,
                facets=facets,
                inputFacets=self.input_facets,  # type: ignore
            )
        if len(self.output_facets):
            return OutputDataset(
                namespace=self.source.name,
                name=self.name,
                facets=facets,
                outputFacets=self.output_facets,  # type: ignore
            )

        return OpenLineageDataset(namespace=self.source.name, name=self.name, facets=facets)
