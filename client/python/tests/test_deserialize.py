# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import attrs
from openlineage.client.deserialize import load_event
from openlineage.client.serde import Serde
from openlineage.client.generated import base, external_query_run
import uuid
import datetime

rich_data = {
    "eventTime": "2023-12-16T14:22:11.949Z",
    "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    "eventType": "COMPLETE",
    "run": {
        "runId": "59fc8906-4a4a-45ab-9a54-9cc2d399e10e",
        "facets": {
            "parent": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json#/$defs/ParentRunFacet",
                "run": {"runId": "daf8bcc1-cc3c-41bb-9251-334cacf698fa"},
                "job": {"namespace": "TESTSchedulerID", "name": "TESTParentJobName4"},
            },
            "spark.logicalPlan": {
                "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                "plan": {"this is my payload": "NULL"},
            },
        },
    },
    "job": {
        "namespace": "TESTSchedulerID",
        "name": "test_user_spark.execute_create_data_source_table_as_select_command.dst_table_test_from_src_table_df",
        "facets": {},
    },
    "inputs": [
        {
            "namespace": "s3a://test_db-db",
            "name": "src_table_test",
            "facets": {
                "dataSource": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                    "name": "s3a://test_db-db",
                    "uri": "s3a://test_db-db",
                },
                "schema": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "randomid", "type": "string"},
                        {"name": "zip", "type": "string"},
                    ],
                },
                "symlinks": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                    "identifiers": [
                        {
                            "namespace": "hive://hive-metastore.hive.svc.cluster.local:9083",
                            "name": "shopify.raw_product_catalog",
                            "type": "TABLE",
                        }
                    ],
                },
            },
            "inputFacets": {},
        }
    ],
    "outputs": [
        {
            "namespace": "s3a://test_db-db",
            "name": "dst_table_test_from_src_table_df",
            "facets": {
                "dataSource": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
                    "name": "s3a://test_db-db",
                    "uri": "s3a://test_db-db",
                },
                "schema": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "randomid", "type": "string"},
                        {"name": "zip", "type": "string"},
                    ],
                },
                "columnLineage": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
                    "fields": {
                        "id": {
                            "inputFields": [
                                {
                                    "namespace": "s3a://test_db-db",
                                    "name": "/src_table_test",
                                    "field": "comments",
                                }
                            ]
                        },
                        "randomid": {
                            "inputFields": [
                                {
                                    "namespace": "s3a://test_db-db",
                                    "name": "/src_table_test",
                                    "field": "products",
                                }
                            ]
                        },
                        "zip": {
                            "inputFields": [
                                {
                                    "namespace": "s3a://test_db-db",
                                    "name": "/src_table_test",
                                    "field": "platform",
                                }
                            ]
                        },
                    },
                },
                "symlinks": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json#/$defs/SymlinksDatasetFacet",
                    "identifiers": [
                        {
                            "namespace": "hive://hive-metastore.hive.svc.cluster.local:9083",
                            "name": "shopify.fact_order_new5",
                            "type": "TABLE",
                        }
                    ],
                },
                "lifecycleStateChange": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.5.0/integration/spark",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet",
                    "lifecycleStateChange": "CREATE",
                },
            },
            "outputFacets": {},
        }
    ],
}


def test_rich_event():
    assert Serde.to_dict(load_event(rich_data)) == rich_data


def test_deserialize():
    event = base.RunEvent(
        eventTime=datetime.datetime.now().isoformat() + "Z",
        eventType=base.EventType.START,
        job=base.Job(name="name", namespace="321"),
        run=base.Run(
            runId=str(uuid.uuid4()),
            facets={"externalQuery": external_query_run.ExternalQueryRunFacet("1", "2")},
        ),
        producer="http://321.com",
    )
    assert load_event(attrs.asdict(event)) == event
