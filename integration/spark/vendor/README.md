# Vendor Projects

This directory hosts vendor-specific projects for integrating with various data sources.
Currently, it includes the Snowflake project, which implements OpenLineage interfaces for getting input and output Snowflake datasets.

## How
Follow a common pattern for vendor integration projects:
1. Implement the `io.openlineage.spark.api.Vendor` interface, this interface will work as gateway to other vendor specific implementation.
2. These are the interface that can be implemented: `io.openlineage.spark.agent.lifecycle.VisitorFactory` and the `io.openlineage.spark.api.OpenLineageEventHandlerFactory`.
3. Additionally, implement the `isVendorAvailable` method to check if the vendor can be loaded.

In the resources folder, add a `META-INF.services` file with the fully qualified class name of the `io.openlineage.spark.api.Vendor` implementation class for auto-discovery.
This is how the Vendor will be loaded.
```java
ServiceLoader<Vendor> serviceLoader = ServiceLoader.load(Vendor.class);
List<Vendor> vendors =
        StreamSupport.stream(
                        serviceLoader.spliterator(),
                        false)
                .filter(Vendor::isVendorAvailable)
                .collect(Collectors.toList());
```

## Why
Separating vendors from the main Spark integration enhances project flexibility,
supporting new vendors and reducing code and dependency coupling.
This separation accommodates variations in Spark and Scala versions supported by different vendors.

Keeping vendor-specific code in separate projects facilitates handling specific cases for integration without a Spark-based source or destination.
It simplifies testing and supporting multiple versions of the same vendor, preserving a clean main codebase.
This approach also lowers the barrier for new contributors implementing new vendors,
allowing loading dependencies from entirely different projects, effectively supporting [plugins](https://github.com/OpenLineage/OpenLineage/issues/2162).

The vendors currently in the OpenLineage Spark connectors are Snowflake, BigQuery, Databricks, Delta, and Iceberg.
Each requires separate dependencies with varying levels of support for Spark and Scala versions.

The primary objective is to organize these connectors into distinct project folders, loading them at runtime,
with each having logic to determine whether it can be loaded or not.
Here's a dependency schema represent the internal classes through which vendors interact with the normal flow:

![dependency_schema.png](docs%2Fassets%2Fdependency_schema.png)

## Roadmap
The Visitor Factory and the Event Handler Factory will be the initial implementations,
followed by the Catalog Handler to support Iceberg.
After separating Snowflake and Iceberg, BigQuery will be relatively easy,
and then the more tightly coupled Delta and Databricks will follow,
requiring additional classes such as JobNameHook and EventFilter.