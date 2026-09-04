# Shared Spark dataset dispatch and tracing

Status: implementation plan.

## Objective

Explain which dataset handlers are checked and invoked for a Spark event, their
nested calls, results, and failures without changing extraction behaviour.

The existing execution is order-dependent and difficult to observe. This plan
does not claim it is inherently non-deterministic. Given the same handler order,
event sequence, and initial state, sequential dispatch can be deterministic even
when an earlier handler changes a later handler's eligibility.

## Scope

Add two internal components:

- a shared dispatcher for existing applicability checks, invocation, exception
  handling, and sequential result collection
- an opt-in, bounded trace sink around those actual operations

Keep the existing visitor and dataset-builder APIs, factories, traversal,
registration order, visited-node behaviour, dataset reduction, and facet merging.
Do not introduce a new extractor API, traversal engine, composition policies,
intermediate dataset model, or registry redesign.

## Dispatcher

Make `PlanUtils.merge()` a thin adapter over the dispatcher. Route the event/node
builder loop through the same dispatcher. Existing nested delegation through
`merge()` then uses the same instrumentation.

Preserve these behaviours:

- composite `isDefinedAt()` stops at the first matching handler
- composite `apply()` checks and invokes handlers sequentially, collecting all
  non-null contributions
- a handler can change visited state before the next handler is checked
- predicates are called exactly where they are today, including repeated checks
- standalone safe application and merged application keep their existing,
  distinct exception and null-result handling
- no extra predicate calls are made to explain a decision

Correct the misleading `merge()` documentation. Do not compute matches upfront,
cache applicability, sort handlers, or introduce fallback priorities.

## Trace sink

Enable tracing through the dedicated `DatasetDispatchTrace` logger at `TRACE`.
It is disabled with normal logging configuration and does not require a new
OpenLineage facet or Spark configuration API.

Open a trace scope inside each input/output extraction callable, including when
that callable runs on a timeout executor thread. Close the scope on success or
failure. Nested dispatch inherits the current scope and parent invocation.

Record:

- a unique capture identifier, run identifier, event class, and input/output phase
- handler implementation, using the underlying builder name for visitor adapters
- invocation and parent identifiers, node class, and scope-local node identity
- actual match outcomes, results as collection counts or null, duration, and
  exception type
- visited-node rejection and the responsible invocation when it was observed in
  the same scope

A proposed trace could contain:

```text
SQL_END / input / node n17: LogicalRelation
  invocation 12: LogicalRelationDatasetBuilder
    match: true
    result: 1 dataset
  invocation 13: AnotherBuilder
    match: false — already visited by invocation 12
```

Legacy predicates that return only a boolean are reported as such. Do not invent
reasons for a non-match. This change explains dispatch, not dataset reduction or
final facet provenance.

Bound each phase to 1,000 trace records plus a truncation notice. Do not log node
contents, plans, dataset values, or exception messages. Identity bookkeeping is
bounded and released when the scope closes. Tracing and sink failures must not
change extraction results. Do not use node or invocation identifiers as metric
labels.

## Implementation sequence

1. Add the dispatcher and trace scope with regression tests.
2. Delegate the existing `PlanUtils` entry points and event/node builder loop to
   the dispatcher, preserving their behaviour.
3. Add scoped tracing inside input/output capture tasks and a visited-state trace
   hook that does not alter suppression.
4. Document logger configuration, trace fields, limits, and known coverage.
5. Run focused dispatch tests and existing builder, visitor, and integration tests
   where the local environment supports them.

## Acceptance criteria

- tracing enabled and disabled produce the same results, handler order, and
  predicate invocation counts
- composite matching short-circuits and application remains sequential
- empty, null, failed, and unmatched outcomes remain distinguishable
- earlier handlers still affect later eligibility through existing visited state
- nested delegation records the real underlying handler and parent invocation
- standalone event/node builders and merged plan visitors share instrumentation
- trace scopes are isolated across worker threads and cleaned after exceptions
- trace volume and identity bookkeeping remain bounded after truncation
- tracing does not evaluate node hashes, render plans, or expose dataset contents
- existing public visitor and builder contracts remain unchanged

Once traces expose a problematic execution path, simplify that path separately.
Do not expand this change into a general extraction-framework redesign.

## Source anchors

- [PlanUtils](../integration/spark/shared/src/main/java/io/openlineage/spark/agent/util/PlanUtils.java)
- [OpenLineageRunEventBuilder](../integration/spark/shared/src/main/java/io/openlineage/spark/agent/lifecycle/OpenLineageRunEventBuilder.java)
- [AbstractQueryPlanDatasetBuilder](../integration/spark/shared/src/main/java/io/openlineage/spark/api/AbstractQueryPlanDatasetBuilder.java)
- [VisitedNodes](../integration/spark/shared/src/main/java/io/openlineage/spark/api/VisitedNodes.java)

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
