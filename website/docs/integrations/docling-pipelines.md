---
sidebar_position: 10
title: Docling pipelines
---

[Docling pipelines](https://github.com/docling-project/docling-pipelines) is an open-source document curation pipeline for Retrieval Augmented Generation (RAG) applications. It ingests documents from sources such as local filesystems, Amazon S3, SharePoint, OneDrive, Google Drive, Box, and web pages, extracts and curates their content using [Docling](https://github.com/docling-project/docling), and writes chunks, entities, and vector embeddings to targets such as Milvus or OpenSearch. Pipelines are defined as JSON flows: DAGs of operators that can be run from the CLI, the Python API, or a REST API.

To learn more about Docling pipelines, visit the [GitHub repository](https://github.com/docling-project/docling-pipelines).

## How does Docling pipelines work with OpenLineage?

Docling pipelines emits OpenLineage run events during flow execution. The orchestrator calls lifecycle hooks at key execution milestones, an observer translates them into OpenLineage events, and the OpenLineage Python client sends them using the configured transport.

The integration is opt-in. No events are emitted unless lineage is explicitly enabled, and there is no overhead when it is disabled. By default, errors in the lineage code are logged as warnings and never stop a pipeline.

Credential keys (such as `api_key`, `secret`, `password`, and `token`) are stripped from the flow definition before any facet is built.

## Emission modes

The granularity of emitted lineage is controlled by the `DOCPIPE_LINEAGE_MODE` environment variable.

| | `flow` (default) | `operator` |
| :--- | :--- | :--- |
| **Flow-level events** | Yes | Yes |
| **Per-node events** | No | Yes, one child run per DAG node |
| **Jobs in the lineage graph** | One job per flow | One job per flow and one job per operator |
| **Event volume** | Low | Higher, proportional to the number of nodes |
| **Best for** | Production monitoring and dashboards | Debugging, per-operator audit, and data quality investigation |

- **Flow mode** emits a single job per pipeline run. It has the ingest source as its input dataset and the terminal output(s) as its output datasets.
- **Operator mode** emits all the flow-level events and, additionally, events for each DAG node. Each node run is linked to the flow run through the [`parent` run facet](../spec/facets/run-facets/parent_run.md), so backends can show operator runs nested inside their parent flow run.

In both modes, lineage is aggregated per run, not per document. You get document and row counts, but not a separate lineage record for each individual file.

## Event types

### Flow-level events

These events are emitted in both modes.

| Event | Trigger | Key facets |
| :--- | :--- | :--- |
| `START` | Pipeline execution begins | `docpipeFlowId`, `docpipeOperators` |
| `RUNNING` | Ingest completes and active processing begins | Input dataset (the ingest source) with schema and row count |
| `COMPLETE` | Pipeline finishes successfully | Output dataset(s) with schema and row count, `docpipeStats` (`totalDocs`, `completedDocs`, `failedDocs`, `skippedDocs`) |
| `FAIL` | Pipeline terminates with an error | `errorMessage` (message only, without a stack trace) |
| `ABORT` | Pipeline is cancelled or stopped | End timestamp |

### Operator-level events

These events are emitted only in `operator` mode, for each DAG node.

| Event | Trigger | Key facets |
| :--- | :--- | :--- |
| `START` | A node begins execution | `jobType` with the operator name, `parent` run facet, input dataset from the predecessor node |
| `COMPLETE` | A node finishes successfully | Output dataset with schema and row count, `docpipeStats` with operator-specific metrics (for example `totalChunks`), `parent` run facet |
| `FAIL` | A node throws an exception | `errorMessage` with the message and the full stack trace, `parent` run facet |
| `OTHER` | A node is skipped | `docpipeSkip` with the reason, `parent` run facet |

### Datasets

Ingest sources and storage sinks are named from the operator's provider configuration, so the same physical location gets the same dataset on both sides of a flow. For example, a local folder is namespace `file` with the folder path as name, and an S3 location is namespace `s3://my-bucket` with the prefix as name. Datasets passed between operators use the `inmemory://` namespace and the name `{flow_name}/{node_name}/output`. Backends use them to connect operator jobs in the lineage graph.

### Standard facets

In addition to the custom facets below, Docling pipelines uses these standard OpenLineage facets:

| Facet | Attached to | Content |
| :--- | :--- | :--- |
| [`jobType`](../spec/facets/job-facets/job-type.md) | Job | `processingType` is `BATCH`, `integration` is `docling-pipelines`, and `jobType` is `FLOW` for flow jobs and `OPERATOR` for operator jobs |
| [`documentation`](../spec/facets/job-facets/documentation.md) | Job | Flow description, for example `Docling-pipelines flow: my_flow` |
| [`schema`](../spec/facets/dataset-facets/schema.md) | Dataset | Columns and types of the table passing through the flow or operator |
| [`outputStatistics`](../spec/facets/dataset-facets/output-dataset-facets/output_statistics.md) | Output dataset | `rowCount` |
| [`parent`](../spec/facets/run-facets/parent_run.md) | Run | Links operator runs to the flow run (`operator` mode only) |

### Custom facets

| Facet | Attached to | Description |
| :--- | :--- | :--- |
| `docpipeFlowId` | Job | The flow's asset ID, or a SHA-256 hash of the credentials-stripped flow definition |
| `docpipeOperators` | Job | The operators in the flow and their count |
| `docpipeStats` | Run | Document counts for a flow run, or operator-specific metrics for a node run |
| `docpipeSkip` | Run | The reason a node was skipped |

## Getting started

### Installation

Install Docling pipelines with the optional `lineage` extra, which includes the OpenLineage Python client:

```bash
pip install "docling-pipelines[lineage]"
```

If lineage is enabled but the OpenLineage client is not installed, a warning is logged and events are discarded. The pipeline continues to run.

### Enabling lineage

Lineage emission is disabled by default. To enable it, set `DOCPIPE_LINEAGE_ENABLED` and run a flow. Without a transport configured, events are printed to the console, which is a good way to inspect them first:

```bash
export DOCPIPE_LINEAGE_ENABLED=true

docling-pipelines --flow-file my_flow.json
```

The same variables work when using the Python API:

```python
import os

os.environ["DOCPIPE_LINEAGE_ENABLED"] = "true"

from docpipe import DocpipeFlowManager

DocpipeFlowManager(flow_file="my_flow.json").execute()
```

### Sending events to an OpenLineage backend

Transport settings are resolved by the OpenLineage Python client from `OPENLINEAGE__`-prefixed environment variables or an `openlineage.yml` file. See [Python client configuration](../client/python/configuration.md) for all available transports. For example, to send operator-level events to a local [Marquez](https://marquezproject.ai) instance:

```bash
export DOCPIPE_LINEAGE_ENABLED=true
export DOCPIPE_LINEAGE_MODE=operator
export DOCPIPE_LINEAGE_NAMESPACE=docpipe://dev
export OPENLINEAGE__TRANSPORT__TYPE=http
export OPENLINEAGE__TRANSPORT__URL=http://localhost:5000

docling-pipelines --flow-file my_flow.json
```

### Configuration options

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DOCPIPE_LINEAGE_ENABLED` | Enables lineage emission | `false` |
| `DOCPIPE_LINEAGE_MODE` | Emission mode, `flow` or `operator` | `flow` |
| `DOCPIPE_LINEAGE_NAMESPACE` | OpenLineage namespace for all jobs and datasets. Use a stable, environment-specific value in shared deployments. | `docpipe://local` |
| `DOCPIPE_LINEAGE_PRODUCER` | Producer URI included in all facets | `https://github.com/docling-project/docling-pipelines` |
| `DOCPIPE_LINEAGE_STRICT` | When `true`, lineage errors are raised and stop the pipeline. Otherwise, they are logged as warnings. | `false` |

## Where can I learn more?

- [Docling pipelines OpenLineage guide](https://github.com/docling-project/docling-pipelines/blob/main/docs/integrations/openlineage/OPENLINEAGE_GUIDE.md)
- [How-to guide on integrating Docling pipelines with Marquez](https://github.com/docling-project/docling-pipelines/blob/main/docs/integrations/openlineage/OPENLINEAGE_GUIDE.md#73-marquez-integration-and-visualization)
- [Know Where Every Document Went: OpenLineage Support in Docling Pipelines](https://medium.com/@msoni6226/know-where-every-document-went-openlineage-support-in-docling-pipelines-d12c04d2c183)
