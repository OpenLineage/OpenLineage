# OpenLineage Explicit Lineage: Adoption Guide

## The Core Problem We're Solving

**Current spec creates false positives.** When a job reads datasets A,B and writes C,D, OpenLineage infers 4 edges: A→C, A→D, B→C, B→D. If the real flow is only A→C and B→D, **50% are false positives**. For bulk ETL processing 10 tables, this becomes **90% false positives**.

**Impact:** Unreliable impact analysis, failed compliance audits, unusable lineage graphs.

## The Solution: Facet-Based Explicit Lineage

Three new facets that let producers explicitly declare data flow:

| Facet | Where | Purpose |
|-------|-------|---------|
| **LineageRunFacet** | run.facets.lineage | Runtime observed flow |
| **LineageJobFacet** | job.facets.lineage | Design-time declared flow |
| **LineageDatasetFacet** | dataset.facets.lineage | Structural derivation (views, synonyms, reports) |

**Key aspect:** Uses existing facet mechanism — no core model changes.

## What It Solves

... there are more situations where the current spec falls short:

### 1. False Positives (Core Problem)
```json
// OLD: 4 edges (2 false positives)
"inputs": ["table_a", "table_b"],
"outputs": ["table_c", "table_d"]

// NEW: 2 correct edges
"run": { "facets": { "lineage": { "entries": [
  {
    "namespace": "...",
    "name": "table_c",
    "type": "DATASET",
    "inputs": [
      {"namespace": "...", "name": "table_a", "type": "DATASET"}
    ]
  },
  {
    "namespace": "...",
    "name": "table_d",
    "type": "DATASET",
    "inputs": [
      {"namespace": "...", "name": "table_b", "type": "DATASET"}
    ]
  }
]}}}
```

### 2. Dataset-to-Dataset (No Job Needed)
```json
// OLD: Requires artificial job
// NEW: Direct on DatasetEvent
"dataset": {
  "namespace": "...",
  "name": "public.my_view",
  "facets": {
    "lineage": {
      "inputs": [
        {"namespace": "...", "name": "public.base_table", "type": "DATASET"}
      ]
    }
  }
}
```

### 3. Mixed Granularity (Dataset + Column)
```json
// OLD: Combining CLL with inputs/outputs with false negatives
// NEW: Unified structure
"run": { "facets": { "lineage": { "entries": [
  {
    "namespace": "...",
    "name": "table_a",
    "type": "DATASET",
    "fields": {
      "column_A": {
        "inputs": [
          {"namespace": "...", "name": "table_b", "type": "DATASET", "field": "column_A"}
        ]
      }
    }
  },
  {
    "namespace": "file://",
    "name": "file_c",
    "type": "DATASET",
    "inputs": [
      {"namespace": "...", "name": "table_b", "type": "DATASET", "field": "column_B"}
    ]
  }
]}}}
```

## When to Use

### Producers: Use `compatibility = both`

**Immediately:** Start emitting new facets while maintaining backward compatibility.

```yaml
openlineage.lineage.compatibility: both
```

This auto-generates old format (inputs/outputs/CLL) from new facets, ensuring old consumers still work.

**Benefits:**
- New consumers get accurate lineage
- Old consumers continue working
- No coordination needed
- Zero breaking changes

### Consumers: Add Support at Your Convenience

**Priority:** Check for new facets first, fall back to old format.

```python
# Precedence order
if event.run.facets.lineage:
    use_explicit_lineage()
elif output.facets.columnLineage:
    use_cll()
else:
    use_cartesian_product()
```

**Timeline:** No rush—producers emit both formats during transition.

## Migration Path

### Phase 1: Producers Use `compatibility = both`
- Emit new facets + old format
- Zero consumer impact
- Immediate accuracy for new consumers

### Phase 2: Consumers Add New Facet Support
- Update at own pace
- Old format still available
- Gradual ecosystem upgrade

### Phase 3: Deprecate Old Format
- Long transition period
- CLL eventually deprecated
- Single unified model

## Key Benefits

| Benefit | Impact |
|---------|--------|
| **Eliminates false positives** | Reliable impact analysis |
| **No artificial entities** | Clean lineage graphs |
| **Unified granularity** | Single model for all cases |
| **Backward compatible** | Zero breaking changes |
| **Uses facets** | No core model changes |
| **Gradual adoption** | No coordination needed |

## Action Items

### For Producers
1. Update to latest OpenLineage client library
    - `openlineage.lineage.compatibility = both` will be set by default
    - New facets will be emitted by default (client auto-generates old format)
    - Done. No consumer coordination needed

### For Consumers
1. Update lineage parsing logic
2. Check for new facets first
3. Fall back to old format
4. Deploy at your convenience
