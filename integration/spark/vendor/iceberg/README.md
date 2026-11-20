# Iceberg Spark Vendor

This package should contain all the Iceberg specific methods and rest of the project 
should not depend on any Iceberg classes. 

As the first step to achieve this, its initial version contains facet builder to build
`InputStatisticsInputDatasetFacet` facet.

## Catalog metadata reads

Iceberg dataset-location and version lookups do not populate the Spark catalog's
shared cache. Otherwise, a lookup after an entry expires during a write can cache
pre-commit metadata and leave the next SQL statement reading an old snapshot.
These lookups reuse the backing catalog with a lookup-local wrapper that preserves
Iceberg's identifier rules. Each lookup reaches the backing catalog rather than
reusing the session's cached table. Path-based tables and catalogs with caching
disabled keep their existing lookup path.

Separate catalog loads, such as the initial table load for ALTER TABLE events,
are outside this path.