---
sidebar_position: 3
title: Circuit Breaker
---

import CircuitBreakers from '@site/docs/client/java/partials/java_circuit_breaker.md';

<CircuitBreakers/>

:::note
When using the `asyncTaskQueue` circuit breaker with Spark, see
[Graceful shutdown](usage.md#graceful-shutdown) for how `shutdownTimeoutSeconds` relates to
Spark's shutdown hook timeout.
:::
