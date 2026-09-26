---
sidebar_position: 8
title: Presto
---

:::info
This integration is available in Presto 0.297 and later.
:::

[Presto](https://prestodb.io/) is an open source distributed SQL query engine
designed for fast analytics across data from multiple sources. Its connectors
allow a single query to access data in systems such as object stores,
relational databases, and data warehouses.

## How does Presto work with OpenLineage?

Presto includes an OpenLineage event listener plugin. The plugin uses Presto's
event listener interface to emit query metadata in the OpenLineage format.

The integration emits query lifecycle events, including `START`,
`COMPLETE`, and `FAIL`, and captures:

- Input and output datasets
- Dataset schemas
- Table-level lineage
- Column-level lineage
- SQL, query context, query statistics, and error metadata

Events can be written to the Presto coordinator output with the console
transport or sent to an OpenLineage-compatible HTTP endpoint.

## Configuring the Presto OpenLineage plugin

The plugin is bundled with Presto and requires no additional installation.

Create `etc/event-listener.properties` on the Presto coordinator:

```properties
event-listener.name=openlineage-event-listener
openlineage-event-listener.presto.uri=__PRESTO_URI__
openlineage-event-listener.transport.type=HTTP
openlineage-event-listener.transport.url=__OPENLINEAGE_URL__
```

Set:

- `__PRESTO_URI__` to the URI of the Presto coordinator. The integration uses
  it when rendering OpenLineage namespaces.
- `__OPENLINEAGE_URL__` to the base URL of the OpenLineage-compatible HTTP
  endpoint.

For local testing, set
`openlineage-event-listener.transport.type=CONSOLE` and omit the HTTP URL.

If you use a custom event listener configuration filename, add it to
`event-listener.config-files` in the Presto coordinator configuration.

### Official documentation

See the
[Presto OpenLineage event listener documentation](https://prestodb.io/docs/current/develop/openlineage-event-listener.html)
for the complete configuration reference.

## Feedback

To report a problem or contribute an improvement, visit the
[Presto GitHub repository](https://github.com/prestodb/presto).

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
