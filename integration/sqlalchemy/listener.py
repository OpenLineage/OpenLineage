# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

# Required: SQLAlchemy 2+
# SQLALCHEMY_DATABASE_URI must be provided as env variable

from datetime import datetime
from typing import List
import os

from sqlalchemy import event, inspect
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import Table
from openlineage.common.dataset import Dataset, Field, Source
from . import adapter, OpenLineageAdapter

class SQLAlchemyCollector:
    start_time: datetime = None
    sqlalchemy_tables: List[Table] = None # SQLAlchemy table
    complete_time: datetime = None
    datasets: List[Dataset] = None
    query_string: str = None

    def assemble_datasets(self) -> List[Dataset]:
        source: Source = Source(
            scheme=os.environ.get('OPENLINEAGE_NAMESPACE'), 
            connection_url='url' # alt: os.environ.get('SQLALCHEMY_DATABASE_URI')
            )
        datasets: List[Dataset] = []
        fields: List[Field] = []
        if self.sqlalchemy_tables:
            for table in self.sqlalchemy_tables:
                name: str = table.name
                for column in table.columns:
                    fields.append(
                        Field(
                            name=column.name,
                            type=str(column.type)
                        )
                    )
                datasets.append(
                    Dataset(
                        name=name,
                        source=source,
                        fields=fields
                    ).to_openlineage_dataset()
                )
            return datasets
        return []

    def collect_metadata(self):
        @event.listens_for(Session, 'after_transaction_create')
        def after_transaction_create(session, transaction):
            """listens for start event"""
            self.start_time = datetime.now().isoformat()
            print('start time: ', self.start_time)

        @event.listens_for(Session, 'do_orm_execute')
        def receive_do_orm_execute(orm_execute_state):
            """retrieves datasets and queries from logged event"""
            for mapper in orm_execute_state.all_mappers:
                nested_mapper = inspect(mapper)
                if self.start_time:
                    self.sqlalchemy_tables = nested_mapper.tables
                    self.query_string = str(orm_execute_state.statement)

            if self.start_time:
                adapter = OpenLineageAdapter()
                adapter.create_events(
                    datasets=self.assemble_datasets(), 
                    query_string=self.query_string,
                    start_eventTime=self.start_time,
                    )

        @event.listens_for(Session, 'after_commit')
        def receive_after_commit(session):
            """listens for complete event"""
            self.complete_time = datetime.now().isoformat()

            if self.complete_time:
                adapter = OpenLineageAdapter()
                adapter.create_events(
                    datasets=self.assemble_datasets(), 
                    query_string=self.query_string,
                    complete_eventTime=self.complete_time
                    )

collector = SQLAlchemyCollector()

