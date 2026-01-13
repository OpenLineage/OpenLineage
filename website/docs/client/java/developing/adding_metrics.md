---
title: Metrics Backends
sidebar_position: 3
---

To integrate additional metrics backend into the OpenLineage client, implement the `MeterRegistryFactory` interface and ensure it is utilized by the `MicrometerProvider`'s `getMetricsBuilders` method.

The `MeterRegistryFactory` interface is designed to construct a `MeterRegistry` object from the OpenLineage configuration map. This interface allows the integration of either custom implementations or existing ones provided by Micrometer.

If your metrics backend requires external dependencies (e.g., `io.micrometer:micrometer-registry-otlp:latest`), add them to your project's build.gradle as compileOnly. This ensures they are available during compilation but optional at runtime.

Use `ReflectionUtils.hasClass` to check the existence of required classes on the classpath before using them. This prevents runtime failures due to missing dependencies.

```
    if (ReflectionUtils.hasClass("io.micrometer.statsd.StatsdMeterRegistry")) {
      builders.add(new StatsDMeterRegistryFactory());
    }
```