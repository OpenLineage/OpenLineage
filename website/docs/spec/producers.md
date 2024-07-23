---
sidebar_position: 6
---

# Producers

:::info
This page could use some extra detail! You're welcome to contribute using the Edit link at the bottom.
:::

The `_producer` value is included in an OpenLineage request as a way to know how the metadata was generated. It is a URI that links to a source code SHA or the location where a package can be found.

For example, this field is populated by many of the common integrations. For example, the dbt integration will set this value to `https://github.com/OpenLineage/OpenLineage/tree/{__version__}/integration/dbt` and the Python client will set it to `https://github.com/OpenLineage/OpenLineage/tree/{__version__}/client/python`.