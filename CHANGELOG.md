# Changelog

## [Unreleased](https://github.com/OpenLineage/OpenLineage/compare/0.8.2...HEAD)

## [0.8.2](https://github.com/OpenLineage/OpenLineage/compare/0.8.1...0.8.2)
### Added
* `openlineage-airflow` now supports getting credentials from [Airflows secrets backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) ([#723](https://github.com/OpenLineage/OpenLineage/pull/723)) [@mobuchowski](https://github.com/mobuchowski)
* `openlineage-spark` now supports [Azure Databricks Credential Passthrough](https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough) ([#595](https://github.com/OpenLineage/OpenLineage/pull/595)) [@wjohnson](https://github.com/wjohnson)
* `openlineage-spark` detects datasets wrapped by `ExternalRDD`s ([#746](https://github.com/OpenLineage/OpenLineage/pull/746)) [@collado-mike](https://github.com/collado-mike)

### Fixed
* `PostgresOperator` fails to retrieve host and conn during extraction ([#705](https://github.com/OpenLineage/OpenLineage/pull/705)) [@sekikn](https://github.com/sekikn)
* SQL parser accepts lists of sql statements ([#734](https://github.com/OpenLineage/OpenLineage/issues/734)) [@mobuchowski](https://github.com/mobuchowski)
* Missing schema when writing to Delta tables in Databricks ([#748](https://github.com/OpenLineage/OpenLineage/pull/748)) [@collado-mike](https://github.com/collado-mike)

## [0.8.1](https://github.com/OpenLineage/OpenLineage/compare/0.7.1...0.8.1)
### Added
* Airflow integration uses [new TaskInstance listener API](https://github.com/apache/airflow/blob/main/docs/apache-airflow/listeners.rst) for Airflow 2.3+ ([#508](https://github.com/OpenLineage/OpenLineage/pull/508)) [@mobuchowski](https://github.com/mobuchowski)
* Support for HiveTableRelation as input source in Spark integration ([#683](https://github.com/OpenLineage/OpenLineage/pull/683)) [@collado-mike](https://github.com/collado-mike)
* Add HTTP and Kafka Client to `openlineage-java` lib ([#480](https://github.com/OpenLineage/OpenLineage/pull/480)) [@wslulciuc](https://github.com/wslulciuc), [@mobuchowski](https://github.com/mobuchowski)
* New SQL parser, used by Postgres, Snowflake, Great Expectations integrations ([#644](https://github.com/OpenLineage/OpenLineage/pull/644)) [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* GreatExpectations: Fixed bug when invoking GreatExpectations using v3 API ([#683](https://github.com/OpenLineage/OpenLineage/pull/689)) [@collado-mike](https://github.com/collado-mike)
 
## [0.7.1](https://github.com/OpenLineage/OpenLineage/compare/0.6.2...0.7.1)
### Added
* Python implements Transport interface - HTTP and Kafka transports are available ([#530](https://github.com/OpenLineage/OpenLineage/pull/530)) [@mobuchowski](https://github.com/mobuchowski)
* Add UnknownOperatorAttributeRunFacet and support in lineage backend ([#547](https://github.com/OpenLineage/OpenLineage/pull/547)) [@collado-mike](https://github.com/collado-mike)
* Support Spark 3.2.1 ([#607](https://github.com/OpenLineage/OpenLineage/pull/607)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Add StorageDatasetFacet to spec ([#620](https://github.com/OpenLineage/OpenLineage/pull/620)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Airflow: custom extractors lookup uses only get_operator_classnames method ([#656](https://github.com/OpenLineage/OpenLineage/pull/656)) [@mobuchowski](https://github.com/mobuchowski)
* README.md created at OpenLineage/integrations for compatibility matrix ([#663](https://github.com/OpenLineage/OpenLineage/pull/663)) [@howardyoo](https://github.com/howardyoo)
 
### Fixed
* Dagster: handle updated PipelineRun in OpenLineage sensor unit test ([#624](https://github.com/OpenLineage/OpenLineage/pull/624)) [@dominiquetipton](https://github.com/dominiquetipton)
* Delta improvements ([#626](https://github.com/OpenLineage/OpenLineage/pull/626)) [@collado-mike](https://github.com/collado-mike)
* Fix SqlDwDatabricksVisitor for Spark2 ([#630](https://github.com/OpenLineage/OpenLineage/pull/630)) [@wjohnson](https://github.com/wjohnson)
* Airflow: remove redundant logging from GE import ([#657](https://github.com/OpenLineage/OpenLineage/pull/657)) [@mobuchowski](https://github.com/mobuchowski)
* Fix Shebang issue in Spark's wait-for-it.sh ([#658](https://github.com/OpenLineage/OpenLineage/pull/658)) [@mobuchowski](https://github.com/mobuchowski)
* Update parent_run_id to be a uuid from the dag name and run_id ([#664](https://github.com/OpenLineage/OpenLineage/pull/664)) [@collado-mike](https://github.com/collado-mike)
* Spark: fix time zone inconsistency in testSerializeRunEvent ([#681](https://github.com/OpenLineage/OpenLineage/pull/681)) [@sekikn](https://github.com/sekikn)

## [0.6.2](https://github.com/OpenLineage/OpenLineage/compare/0.6.1...0.6.2)
### Added
* CI: add integration tests for Airflow's SnowflakeOperator and dbt-snowflake [@mobuchowski](https://github.com/mobuchowski)
* Introduce DatasetVersion facet in spec [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Airflow: add external query id facet [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* Complete Fix of Snowflake Extractor get_hook() Bug [@denimalpaca](https://github.com/denimalpaca)
* Update artwork [@rossturk](https://github.com/rossturk)
* Airflow tasks in a DAG now report a common ParentRunFacet [@collado-mike](https://github.com/collado-mike)

## [0.6.1](https://github.com/OpenLineage/OpenLineage/compare/0.6.0...0.6.1)
### Fixed
* Catch possible failures when emitting events and log them [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* dbt: jinja2 code using do extensions does not crash [@mobuchowski](https://github.com/mobuchowski)


## [0.6.0](https://github.com/OpenLineage/OpenLineage/compare/0.5.2...0.6.0)
### Added
* Extract source code of PythonOperator code similar to SQL facet [@mobuchowski](https://github.com/mobuchowski)
* Add DatasetLifecycleStateDatasetFacet to spec [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Airflow: extract source code from BashOperator [@mobuchowski](https://github.com/mobuchowski)
* Add generic facet to collect environmental properties (EnvironmentFacet) [@harishsune](https://github.com/harishsune)
* OpenLineage sensor for OpenLineage-Dagster integration [@dalinkim](https://github.com/dalinkim)
* Java-client: make generator generate enums as well [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Added `UnknownOperatorAttributeRunFacet` to Airflow integration to record operators that don't produce lineage [@collado-mike](https://github.com/collado-mike)

### Fixed
* Airflow: increase import timeout in tests, fix exit from integration [@mobuchowski](https://github.com/mobuchowski)
* Reduce logging level for import errors to info [@rossturk](https://github.com/rossturk)
* Remove AWS secret keys and extraneous Snowflake parameters from connection uri [@collado-mike](https://github.com/collado-mike)
* Convert to LifecycleStateChangeDatasetFacet [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)

## [0.5.2](https://github.com/OpenLineage/OpenLineage/compare/0.5.1...0.5.2)
### Added

* Proxy backend example using `Kafka` [@wslulciuc](https://github.com/wslulciuc)
* Support Databricks Delta Catalog naming convention with DatabricksDeltaHandler [@wjohnson](https://github.com/wjohnson)
* Add javadoc as part of build task [@mobuchowski](https://github.com/mobuchowski)
* Include TableStateChangeFacet in non V2 commands for Spark [@mr-yusupov](https://github.com/mr-yusupov)
* Support for SqlDWRelation on Databricks' Azure Synapse/SQL DW Connector [@wjohnson](https://github.com/wjohnson)
* Implement input visitors for v2 commands [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Enabled SparkListenerJobStart events to trigger open lineage events [@collado-mike](https://github.com/collado-mike)

### Fixed

* dbt: job namespaces for given dbt run match each other [@mobuchowski](https://github.com/mobuchowski)
* Fix Breaking SnowflakeOperator Changes from OSS Airflow [@denimalpaca](https://github.com/denimalpaca)
* Made corrections to account for DeltaDataSource handling [@collado-mike](https://github.com/collado-mike)

## [0.5.1](https://github.com/OpenLineage/OpenLineage/compare/0.4.0...0.5.1)
### Added
* Support for dbt-spark adapter [@mobuchowski](https://github.com/mobuchowski)
* **New** `backend` to proxy OpenLineage events to one or more event streams 🎉 [@mandy-chessell](https://github.com/mandy-chessell) [@wslulciuc](https://github.com/wslulciuc)
* Add Spark extensibility API with support for custom Dataset and custom facet builders [@collado-mike](https://github.com/collado-mike)

### Fixed
* airflow: fix import failures when dependencies for bigquery, dbt, great_expectations extractors are missing [@lukaszlaszko](https://github.com/lukaszlaszko)
* Fixed openlineage-spark jar to correctly rename bundled dependencies [@collado-mike](https://github.com/collado-mike)

## [0.4.0](https://github.com/OpenLineage/OpenLineage/releases/tag/0.4.0) - 2021-12-13

### Added
* Spark output metrics [@OleksandrDvornik](https://github.com/OleksandrDvornik)
* Separated tests between Spark 2 & 3 [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Databricks install README and init scripts [@wjohnson](https://github.com/wjohnson)
* Iceberg integration with unit tests [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Kafka read and write support [@OleksandrDvornik](https://github.com/OleksandrDvornik) / [@collado-mike](https://github.com/collado-mike)
* Arbitrary parameters supported in HTTP URL construction [@wjohnson](https://github.com/wjohnson)
* Increased visitor coverage for Spark commands [@mobuchowski](https://github.com/mobuchowski) / [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)

### Fixed
* dbt: column descriptions are properly filled from metadata.json [@mobuchowski](https://github.com/mobuchowski)
* dbt: allow parsing artifacts with version higher than officially supported  [@mobuchowski](https://github.com/mobuchowski)
* dbt: dbt build command is supported  [@mobuchowski](https://github.com/mobuchowski)
* dbt: fix crash when build command is used with seeds in dbt 1.0.0rc3 [@mobuchowski](https://github.com/mobuchowski)
* spark: increase logical plan visitor coverage [@mobuchowski](https://github.com/mobuchowski) 
* spark: fix logical serialization recursion issue [@OleksandrDvornik](https://github.com/OleksandrDvornik)
* Use URL#getFile to fix build on Windows [@mobuchowski](https://github.com/mobuchowski)

## [0.3.1](https://github.com/OpenLineage/OpenLineage/releases/tag/0.3.1) - 2021-10-21

### Fixed
* fix import in spark3 visitor [@mobuchowski](https://github.com/mobuchowski)

## [0.3.0](https://github.com/OpenLineage/OpenLineage/releases/tag/0.3.0) - 2021-10-21

### Added
* Spark3 support [@OleksandrDvornik](https://github.com/OleksandrDvornik) / [@collado-mike](https://github.com/collado-mike)
* LineageBackend for Airflow 2 [@mobuchowski](https://github.com/mobuchowski)
* Adding custom spark version facet to spark integration [@OleksandrDvornik](https://github.com/OleksandrDvornik)
* Adding dbt version facet [@mobuchowski](https://github.com/mobuchowski)
* Added support for Redshift profile [@AlessandroLollo](https://github.com/AlessandroLollo)

### Fixed

* Sanitize JDBC URLs [@OleksandrDvornik](https://github.com/OleksandrDvornik)
* strip openlineage url in python client [@OleksandrDvornik](https://github.com/OleksandrDvornik)
* deploy spec if spec file changes [@mobuchowski](https://github.com/mobuchowski)

## [0.2.3](https://github.com/OpenLineage/OpenLineage/releases/tag/0.2.3) - 2021-10-07

### Fixed

* Add dbt `v3` manifest support [@mobuchowski](https://github.com/mobuchowski)

## [0.2.2](https://github.com/OpenLineage/OpenLineage/releases/tag/0.2.2) - 2021-09-08

### Added
* Implement OpenLineageValidationAction for Great Expectations [@collado-mike](https://github.com/collado-mike)
* facet: add expectations assertions facet [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* airflow: pendulum formatting fix, add tests [@mobuchowski](https://github.com/mobuchowski)
* dbt: do not emit events if run_result file was not updated [@mobuchowski](https://github.com/mobuchowski)

## [0.2.1](https://github.com/OpenLineage/OpenLineage/releases/tag/0.2.1) - 2021-08-27

### Fixed

* Default `--project-dir` argument to current directory in `dbt-ol` script [@mobuchowski](https://github.com/mobuchowski)

## [0.2.0](https://github.com/OpenLineage/OpenLineage/releases/tag/0.2.0) - 2021-08-23

### Added

* Parse dbt command line arguments when invoking `dbt-ol` [@mobuchowski](https://github.com/mobuchowski). For example:

  ```
  $ dbt-ol run --project-dir path/to/dir
  ```

* Set `UnknownFacet` for spark (captures metadata about unvisited nodes from spark plan not yet supported) [@OleksandrDvornik](https://github.com/OleksandrDvornik)

### Changed

* Remove `model` from dbt job name [@mobuchowski](https://github.com/mobuchowski)
* Default dbt job namespace to output dataset namespace [@mobuchowski](https://github.com/mobuchowski)
* Rename `openlineage.spark.*` to `io.openlineage.spark.*` [@OleksandrDvornik](https://github.com/OleksandrDvornik)

### Fixed

* Remove instance references to extractors from DAG and avoid copying log property for serializability [@collado-mike](https://github.com/collado-mike)

## [0.1.0](https://github.com/OpenLineage/OpenLineage/releases/tag/0.1.0) - 2021-08-12

OpenLineage is an _Open Standard_ for lineage metadata collection designed to record metadata for a job in execution. The initial public release includes:

* **An inital specification.** The the inital version [`1-0-0`](https://github.com/OpenLineage/OpenLineage/blob/0.1.0/spec/OpenLineage.md) of the OpenLineage specification defines the core model and facets.
* **Integrations** that collect lineage metadata as OpenLineage events:
  * [`Apache Airflow`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow) with support for BigQuery, Great Expectations, Postgres, Redshift, Snowflake
  * [`Apache Spark`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark)
  * [`dbt`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/dbt)
* **Clients** that send OpenLineage events to an HTTP backend. Both [`java`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java) and [`python`](https://github.com/OpenLineage/OpenLineage/tree/main/client/python) are initially supported.
