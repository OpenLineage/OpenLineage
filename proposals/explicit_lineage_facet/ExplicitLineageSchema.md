# Explicit Lineage: Proposed Schema

This file contains the proposed schema details for [ExplicitLineageProposal.md](ExplicitLineageProposal.md).

## LineageDatasetFacet (DatasetFacet)

The natural facet. Lives on datasets in DatasetEvent. The target entity is implicit — it's the dataset the facet is attached to.

```json
"LineageDatasetFacet": {
  "type": "object",
  "description": "Explicit lineage for this dataset. Describes which source entities feed into it, at entity and/or column granularity. Supersedes ColumnLineageDatasetFacet.",
  "allOf": [{ "$ref": "#/$defs/DatasetFacet" }],
  "properties": {
    "inputs": {
      "type": "array",
      "description": "Dataset-level source inputs. When a source includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY) where that source column affects the entire target dataset.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    },
    "fields": {
      "type": "object",
      "description": "Column-level lineage. Maps target field names to their source inputs.",
      "additionalProperties": {
        "$ref": "#/$defs/LineageFieldEntry"
      }
    }
  }
}
```

> Note: no `namespace`, `name`, or `type` on this facet — the target is the dataset it's attached to.

## LineageRunFacet (RunFacet)

TODO: remove

Lives on the run object in RunEvent. Contains an array of `LineageEntry` objects, each identifying a target entity explicitly by namespace, name, and type, along with its source inputs and optional column-level detail.

```json
"LineageRunFacet": {
  "type": "object",
  "description": "Explicit lineage observed during this run. Describes data flow between entities at entity and/or column granularity.",
  "allOf": [{ "$ref": "#/$defs/RunFacet" }],
  "properties": {
    "entries": {
      "type": "array",
      "description": "Lineage entries describing data flow observed during this run.",
      "items": {
        "$ref": "#/$defs/LineageEntry"
      }
    }
  },
  "required": ["entries"]
}
```

## LineageJobFacet (JobFacet)

Lives on the job object in JobEvent. Same structure as LineageRunFacet.

```json
"LineageJobFacet": {
  "type": "object",
  "description": "Explicit lineage declared for this job definition. Describes designed/static data flow at entity and/or column granularity.",
  "allOf": [{ "$ref": "#/$defs/JobFacet" }],
  "properties": {
    "entries": {
      "type": "array",
      "description": "Lineage entries describing data flow designed for this job.",
      "items": {
        "$ref": "#/$defs/LineageEntry"
      }
    }
  },
  "required": ["entries"]
}
```

## Shared Types

### LineageEntry (discriminated union)

TODO: when LineageJobFacet coalesces to LineageRunFacet, we don't need this

Used by LineageRunFacet and LineageJobFacet. Each entry describes ONE target entity and what feeds into it. The `type` field discriminates between dataset targets and job targets. Dataset targets may be fed by datasets or by explicit upstream jobs; job targets may be fed by datasets or jobs when the target job must be identified independently from the event's own job:

```json
"LineageEntry": {
  "oneOf": [
    { "$ref": "#/$defs/LineageDatasetEntry" },
    { "$ref": "#/$defs/LineageJobEntry" }
  ]
}
```

The `enum` constraint on each subtype's `type` field (`"DATASET"` or `"JOB"`) ensures only one branch can match during validation.

### LineageDatasetEntry

A lineage target that is a dataset. Supports both entity-level and column-level lineage from source entities:

```json
"LineageDatasetEntry": {
  "type": "object",
  "description": "Describes data flowing into a target dataset from source entities, at entity and/or column granularity.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the target dataset"
    },
    "name": {
      "type": "string",
      "description": "The name of the target dataset"
    },
    "type": {
      "type": "string",
      "enum": ["DATASET"],
      "description": "Must be DATASET"
    },
    "inputs": {
      "type": "array",
      "description": "Entity-level inputs feeding this target dataset. Each item is a LineageInput (dataset or job). When an input includes a 'field' property, it represents a dataset-wide operation (e.g., GROUP BY) where that input's column affects the entire target dataset.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    },
    "fields": {
      "type": "object",
      "description": "Column-level lineage. Maps target field names to their source inputs.",
      "additionalProperties": {
        "$ref": "#/$defs/LineageFieldEntry"
      }
    }
  },
  "required": ["namespace", "name", "type"]
}
```

### LineageJobEntry

A lineage target that is a job. Used when the target job must be identified explicitly, such as **job-to-job chains** or other cross-job handoffs with no intermediate tracked dataset. It is not used to model the ordinary sink case for the event's own job; that is represented by input datasets that appear in the event `inputs` array but do not feed any tracked output dataset. Does not support column-level lineage (`fields`), but supports an optional `runId` to tie to a specific execution:

When we describe current job, we don't 

```json
"LineageJobEntry": {
  "type": "object",
  "description": "Describes data flowing into a target job. Used when the target job must be identified explicitly.",
  "properties": {
    "type": {
      "type": "string",
      "enum": ["JOB"],
      "description": "Must be JOB"
    },
    "runId": {
      "type": "string",
      "format": "uuid",
      "description": "Optional. The specific run ID of the target job, when the lineage is tied to a particular execution."
    },
    "inputs": {
      "type": "array",
      "description": "Source inputs feeding into this job.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    }
  },
  "required": ["type"]
}
```

### LineageFieldEntry

```json
"LineageFieldEntry": {
  "type": "object",
  "description": "Column-level lineage for a single target field.",
  "properties": {
    "inputs": {
      "type": "array",
      "description": "Source entities and/or fields that feed into this target field.",
      "items": {
        "$ref": "#/$defs/LineageInput"
      }
    }
  },
  "required": ["inputs"]
}
```

### LineageInput (discriminated union)

A source entity that feeds data into a lineage target. The `type` field discriminates between dataset sources and job sources:

```json
"LineageInput": {
  "oneOf": [
    { "$ref": "#/$defs/LineageDatasetInput" },
    { "$ref": "#/$defs/LineageJobInput" }
  ]
}
```

Same pattern — the `enum` on each subtype's `type` field disambiguates.

### LineageDatasetInput

A source input that is a dataset. Supports optional field reference and transformations:

```json
"LineageDatasetInput": {
  "type": "object",
  "description": "A source dataset that feeds data into a lineage target.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the source dataset"
    },
    "name": {
      "type": "string",
      "description": "The name of the source dataset"
    },
    "type": {
      "type": "string",
      "enum": ["DATASET"],
      "description": "Must be DATASET"
    },
    "field": {
      "type": "string",
      "description": "The specific field/column of the source dataset. Optional — when omitted at column level, means 'source column unknown'."
    },
    "transformations": {
      "type": "array",
      "description": "Transformations applied to the source data. Same structure as ColumnLineageDatasetFacet.",
      "items": {
        "$ref": "#/$defs/LineageTransformation"
      }
    }
  },
  "required": ["namespace", "name", "type"]
}
```

### LineageJobInput

A source input that is a job. Used when data comes from an explicitly identified job without an intermediate tracked dataset, including `JOB → JOB` and `JOB → DATASET` chains. Supports an optional `runId` to tie to a specific execution, and optional `transformations` describing how the job produced the data.

`**namespace` and `name` are OPTIONAL.** When both are omitted, the source job is implicitly the **event's own job** — the job carried in the event's `job` field. This is the home for field-level and transformation detail on data produced by the implicit job (the generator case): a target field can name the current job as its source and attach `transformations` without that job having to be restated as an explicit entity. When `namespace`/`name` are present, they identify a different, explicitly tracked job (the `JOB → JOB` / `JOB → DATASET` chain case).

```json
"LineageJobInput": {
  "type": "object",
  "description": "A source job that feeds data into a lineage target. Used when data comes from another job without an intermediate tracked dataset. When namespace/name are omitted, the source is the event's own job.",
  "properties": {
    "namespace": {
      "type": "string",
      "description": "The namespace of the source job. When omitted together with 'name', the source is the event's own job."
    },
    "name": {
      "type": "string",
      "description": "The name of the source job. When omitted together with 'namespace', the source is the event's own job."
    },
    "type": {
      "type": "string",
      "enum": ["JOB"],
      "description": "Must be JOB"
    },
    "runId": {
      "type": "string",
      "format": "uuid",
      "description": "Optional. The specific run ID of the source job, when the lineage is tied to a particular execution."
    },
    "transformations": {
      "type": "array",
      "description": "Transformations applied by the source job to produce the data.",
      "items": {
        "$ref": "#/$defs/LineageTransformation"
      }
    }
  },
  "required": ["type"]
}
```

> Either both `namespace` and `name` are present (an explicitly identified job) or both are absent (the event's own job). A producer SHOULD NOT emit one without the other.

TODO: explain that when refering to current job, we are inferring current namespace/name/runId from the job on the top-level event

### LineageTransformation

Reuses the existing CLL transformation structure:

```json
"LineageTransformation": {
  "type": "object",
  "properties": {
    "type": {
      "type": "string",
      "description": "DIRECT or INDIRECT"
    },
    "subtype": {
      "type": "string",
      "description": "E.g., IDENTITY, AGGREGATION, FILTER, JOIN, etc."
    },
    "description": {
      "type": "string"
    },
    "masking": {
      "type": "boolean"
    }
  },
  "required": ["type"]
}
```

