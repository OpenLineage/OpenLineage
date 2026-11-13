# Proposal: Producer Verification

**Issue**:   
**Status**: Draft  
**Date**: 2026-11-11


## Problem Statement

As the ecosystem of OpenLineage integrations grows and the OpenLineage specification evolves at the same time, it becomes progresively more difficult to understand the depth of support of the OpenLineage standard by individaul producers and consumers.

As a consequence, it is difficult to predict which producers and consumers are likely to integrate well. 

The depth of support can be defined on multiple levels and dimensions. The most impactful ones seem to be: 

1. _Major version_: what (major) version of OpenLineage specification is supported? Current major version of the specification is 2.x, but many producers are still on version 1.x. On the other hand, there might be OpenLineage consumers that do not yet support specification 2.x. 
2. _Supported event types_:  What are the event types that the interation supports? Do the producers/consumers support runtime oriented Run events, design-time oriented Job and Dataset events, or both? 
3. _Naming Convetions Compliance_: Does the integration support documented naming conventions? Does it use conflicting naming conventions? 
4. _Data Lineage Granularity_: Does the integration support the most detailed column-level lineage, dataset-level lineage, or is no lineage information at all captured? 
5. _Data Lineage Capture Method_: Does the integration support Explicit Lineage facet, Column Lineage Facet, or only implicit inputs/outputs relationships? 
6. _Job Granularity_: Does the integration support only single level of Jobs, or does it support more complex sub-hierarchies of Jobs using Paret Run or Parent Job facet?  
7. _Static Metadata_: Does the integration support static metadata like Documentation, Schema, and similar facets? 
8. _Runtime Data Observability Metrics_: Does the integrartion support data observability metrics collected at runtime like Input Statistics and Output Statistics?  

## Example: Incompatible Producer and Consumer

| Dimension | Producer (e.g. a legacy batch integration) | Consumer (e.g. a column-level lineage catalog) |
|---|---|---|
| Major version | 1.x | 2.x only |
| Supported event types | Run events | Run events |
| Data lineage granularity | Dataset-level only | Column-level required |
| Job granularity | Parent Job/Run facets are used to document sub-jobs | Does not support Parent Job/Run facets to establish job hierarchy |
| Runtime observability metrics | None | Displays Input/Output Statistics |

Both sides speak OpenLineage and exchange events without transport errors, yet the integration is of little use:

- Events are accepted or silently dropped, depending on how the consumer handles the 1.x schema.
- The consumer's column-level lineage views stay empty, because the producer never emits the Column Lineage facet.
- Every job appears as a standalone node, as parent facets are not supported by consumer.
- The mismatch is only discovered after deployment, by users noticing missing lineage.

Verifying producer/consumer compatibility up front would reveal these gaps before integration, but that cannot be done at scale today. Details about producer/consumer maturity are often not documented, and verification of compatibility is a significant effort that needs to be done for each combination of OpenLineage producer and consumer - there is no way how to do that in scale. 


## Solution for Producers

The OpenLineage website will host a validation API and a web UI. Producer developers send a sample of events emitted by their integration (for example, events captured from a test run) and receive back:

1. **Validation warnings** - human-readable findings about events that are technically valid, but do not follow the specification's recommendations. Examples: dataset names or namespaces that do not follow the [naming conventions](../../spec/Naming.md), a Parent Run facet referencing a job that is never emitted, or a facet with a deprecated schema version.
2. **Validation footprint** - a simple JSON object that documents the producer's depth of support along the dimensions listed in the Problem Statement. The footprint is derived only from the events submitted, so it describes what the submitted events demonstrate, not what the producer could support in other scenarios. It is the producers responsibility to run the validaiton on representative sample of events.

The API is stateless: submitted events are validated and discarded, and only the response is returned. The UI is a thin layer over the same API: users upload or paste events and see warnings and the footprint rendered side by side. Both are part of the OpenLineage website.

### Example footprint

```json
{
  "specMajorVersion": 1,
  "eventTypes": ["RUN"],
  "namingConventions": {
    "compliant": false,
    "violations": ["Dataset namespace 'my-bucket' does not match the S3 convention 's3://{bucket}'"]
  },
  "lineageGranularity": ["DATASET"],
  "lineageCaptureMethods": ["IMPLICIT_INPUTS_OUTPUTS"],
  "jobGranularity": "HIERARCHICAL",
  "staticMetadata": ["schema"],
  "runtimeMetrics": []
}
```

Each field maps to one dimension: `specMajorVersion` (1), `eventTypes` (2), `namingConventions` (3), `lineageGranularity` (4), `lineageCaptureMethods` (5), `jobGranularity` (6), `staticMetadata` (7), `runtimeMetrics` (8). The exact schema and allowed values are to be defined as part of implementation.

### Example warning

```json
{
  "severity": "WARNING",
  "eventIndex": 3,
  "path": "$.outputs[0].namespace",
  "code": "NAMING_CONVENTION_VIOLATION",
  "message": "Dataset namespace does not follow the S3 naming convention."
}
```

Warnings never fail the validation of a structurally valid event; schema errors are reported separately with severity `ERROR`.



## Solution for Consumers

A new optional endpoint is added to the [OpenLineage API specification](https://openlineage.io/apidocs/openapi/#/openlineage), next to the existing `/lineage` and `/lineage/batch` endpoints. Consumers that implement it can be asked whether they support a given producer. Adding an optional endpoint is a backwards compatible change.

- **Request**: `POST /lineage/footprint/evaluate` with the Producer Footprint (as returned by the producer validation API) as the request body.
- **Response**: the consumer's assessment of the footprint, at two levels of granularity:
  1. **Overall** - a single `supported` flag saying whether the producer is supported by the consumer.
  2. **Details** - an assessment of each entry of the footprint. For example, if the footprint lists three static metadata facets, the consumer marks each of them as supported or not.

The `supported` flag is decided by the consumer. A consumer may report `supported: true` even if some details are not supported, as long as the unsupported entries are not essential to it.

### Example

Request (the footprint from the producer example):

```json
{
  "specMajorVersion": 1,
  "eventTypes": ["RUN"],
  "lineageGranularity": ["DATASET"],
  "lineageCaptureMethods": ["IMPLICIT_INPUTS_OUTPUTS"],
  "jobGranularity": "HIERARCHICAL",
  "staticMetadata": ["schema", "documentation", "ownership"],
  "runtimeMetrics": []
}
```

Response:

```json
{
  "supported": false,
  "details": {
    "specMajorVersion": { "1": "NOT_SUPPORTED" },
    "eventTypes": { "RUN": "SUPPORTED" },
    "lineageGranularity": { "DATASET": "SUPPORTED" },
    "lineageCaptureMethods": { "IMPLICIT_INPUTS_OUTPUTS": "SUPPORTED" },
    "jobGranularity": { "HIERARCHICAL": "NOT_SUPPORTED" },
    "staticMetadata": {
      "schema": "SUPPORTED",
      "documentation": "SUPPORTED",
      "ownership": "NOT_SUPPORTED"
    },
    "runtimeMetrics": {}
  }
}
```

The `details` object mirrors the structure of the footprint: each value listed in the footprint is returned with a status, so producers and users can see exactly which entries cause a mismatch. The set of statuses (for example `SUPPORTED`, `NOT_SUPPORTED`, `PARTIALLY_SUPPORTED`) and the exact request and response schemas are to be defined as part of implementation, in the OpenLineage API specification.

The endpoint is stateless and side-effect free, so any consumer can host it without changing how it stores lineage. Together with the producer validation API, it allows compatibility between any producer and consumer to be checked with two API calls (producer validation, then consumer evaluation), instead of a manual, per-combination effort.