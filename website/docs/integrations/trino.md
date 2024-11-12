---
sidebar_position: 7
title: Trino
---

:::info
This integration is known to work with Trino 450 and later.
:::

Trino is a distributed SQL query engine targeted for big data analytical workloads. Trino queries are typically run on 
Trino `cluster`, where distributed set of Trino `workers` provides compute power and Trino `coordinator` is responsible 
for query submission. By a rich set of available connectors, you can use Trino to execute SQL queries with the same exact
syntax [on different underlying systems](https://trino.io/docs/current/connector.html) - such as RDBMs databases, hive metastore, s3 and others.

Trino enables running queries for fetching the data as well as creating new structures - such as tables, views or materialized views.

To learn more about Trino, visit their [documentation site](https://trino.io/docs/current/).

## How does Trino work with OpenLineage?

Collecting lineage in Trino requires configuring a `plugin`, which will use `EventListener` interface of Trino to extract 
lineage information from metadata available for this interface.

Trino OpenLineage Event Listener plugin will yield 2 events for each executed query - one for STARTED and one for 
SUCCEEDED/FAILED query. While first one already provides us with new job information, actual lineage information 
(inlets/outlets) will be available in the latter event.

This plugin supports both table and column level lineage.

## Configuring Trino OpenLineage plugin

1. Create configuration file named `openlineage-event-listener.properties`

```properties
event-listener.name=openlineage
openlineage-event-listener.transport.type=HTTP
openlineage-event-listener.transport.url=__OPENLINEAGE_URL__
openlineage-event-listener.trino.uri=__TRINO_URI__
```

Make sure to set:
- `__OPENLINEAGE_URL__` - address where OpenLineage API is reachable so plugin can post lineage information.
- `__TRINO_URI__` - address (preferably DNS) of a Trino cluster. It will be used for rendering dataset namespace.

2. Extend properties file used to configure Trino **coordinator** with following line:

```properties
event-listener.config-files=etc/openlineage-event-listener.properties
```

Make sure that the path to `event-listener.config-files` is recognizable by Trino coordinator.

### Official documentation

Current documentation on Trino OpenLineage Event Listener with full list of available configuration options
[is maintained here](https://trino.io/docs/current/admin/event-listeners-openlineage.html).

## Feedback

What did you think of this guide? You can reach out to us on [slack](https://join.slack.com/t/openlineage/shared_invite/zt-2u4oiyz5h-TEmqpP4fVM5eCdOGeIbZvA) and leave us feedback!  
