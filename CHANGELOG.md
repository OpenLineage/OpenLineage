# Changelog

## [Unreleased](https://github.com/OpenLineage/OpenLineage/compare/0.19.2...HEAD)

## [0.19.2](https://github.com/OpenLineage/OpenLineage/compare/0.18.0...0.19.2) - 2023-1-4
### Added
* Airflow: add Trino extractor [`#1288`](https://github.com/OpenLineage/OpenLineage/pull/1288) [@sekikn](https://github.com/sekikn)  
    *Adds a Trino extractor to the Airflow integration.*
* Airflow: add `S3FileTransformOperator` extractor [`#1450`](https://github.com/OpenLineage/OpenLineage/pull/1450) [@sekikn](https://github.com/sekikn)  
    *Adds an `S3FileTransformOperator` extractor to the Airflow integration.*
* Airflow: add standardized run facet [`#1413`](https://github.com/OpenLineage/OpenLineage/pull/1413) [@JDarDagran](https://github.com/JDarDagran)  
    *Creates one standardized run facet for the Airflow integration.*
* Airflow: add `NominalTimeRunFacet` and `OwnershipJobFacet` [`#1410`](https://github.com/OpenLineage/OpenLineage/pull/1410) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds `nominalEndTime` and `OwnershipJobFacet` fields to the Airflow integration.*
* dbt: add support for postgres datasources [`#1417`](https://github.com/OpenLineage/OpenLineage/pull/1417) [@julienledem](https://github.com/julienledem)  
    *Adds the previously unsupported postgres datasource type.*
* Proxy: add client-side proxy (skeletal version) [`#1439`](https://github.com/OpenLineage/OpenLineage/pull/1439) [`#1420`](https://github.com/OpenLineage/OpenLineage/pull/1420) [@fm100](https://github.com/fm100)  
    *Implements a skeletal version of a client-side proxy.*
* Proxy: add CI job to publish Docker image [`#1086`](https://github.com/OpenLineage/OpenLineage/pull/1086) [@wslulciuc](https://github.com/wslulciuc)   
    *Includes a script to build and tag the image plus jobs to verify the build on every CI run and publish to Docker Hub.*
* SQL: add `ExtractionErrorRunFacet` [`#1442`](https://github.com/OpenLineage/OpenLineage/pull/1442) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds a facet to the spec to reflect internal processing errors, especially failed or incomplete parsing of SQL jobs.*
* SQL: add column-level lineage to SQL parser [`#1432`](https://github.com/OpenLineage/OpenLineage/pull/1432) [`#1461`](https://github.com/OpenLineage/OpenLineage/pull/1461) [@mobuchowski](https://github.com/mobuchowski) [@StarostaGit](https://github.com/StarostaGit)  
    *Adds support for extracting column-level lineage from SQL statements in the parser, including adjustments to Rust-Python and Rust-Java interfaces and the Airflow integration's SQL extractor to make use of the feature. Also includes more tests, removal of the old parser, and removal of the common-build cache in CI (which was breaking the parser).*
* Spark: pass config parameters to the OL client [`#1383`](https://github.com/OpenLineage/OpenLineage/pull/1383) [@tnazarew](https://github.com/tnazarew)  
    *Adds a mechanism for making new lineage consumers transparent to the integration, easing the process of setting up new types of consumers.*

### Fixed
* Airflow: fix `collect_ignore`, add flags to Pytest for cleaner output [`#1437`](https://github.com/OpenLineage/OpenLineage/pull/1437) [@JDarDagran](https://github.com/JDarDagran)  
    *Removes the `extractors` directory from the ignored list, improving unit testing.*
* Spark & Java client: fix README typos [@versaurabh](https://github.com/versaurabh)  
    *Fixes typos in the SPDX license headers.*


## [0.18.0](https://github.com/OpenLineage/OpenLineage/compare/0.17.0...0.18.0) - 2022-12-8
### Added
* Airflow: support `SQLExecuteQueryOperator` [`#1379`](https://github.com/OpenLineage/OpenLineage/pull/1379) [@JDarDagran](https://github.com/JDarDagran)  
    *Changes the `SQLExtractor` and adds support for the dynamic assignment of extractors based on `conn_type`.*
* Airflow: introduce a new extractor for `SFTPOperator` [`#1263`](https://github.com/OpenLineage/OpenLineage/pull/1263) [@sekikn](https://github.com/sekikn)  
    *Adds an extractor for tracing file transfers between local file systems.*
* Airflow: add Sagemaker extractors [`#1136`](https://github.com/OpenLineage/OpenLineage/pull/1136) [@fhoda](https://github.com/fhoda)  
    *Creates extractors for `SagemakerProcessingOperator` and `SagemakerTransformOperator`.*
* Airflow: add S3 extractor for Airflow operators [`#1166`](https://github.com/OpenLineage/OpenLineage/pull/1166) [@fhoda](https://github.com/fhoda)  
    *Creates an extractor for the `S3CopyObject` in the Airflow integration.*  
* Airflow: implement DagRun listener [`#1286`](https://github.com/OpenLineage/OpenLineage/pull/1286) [@mobuchowski](https://github.com/mobuchowski)  
    *OpenLineage integration will now explicitly emit DagRun start and DagRun complete or DagRun failed events, which allows precise tracking of single dags.*
* Spec: add spec file for `ExternalQueryRunFacet` [`#1262`](https://github.com/OpenLineage/OpenLineage/pull/1262) [@howardyoo](https://github.com/howardyoo)  
    *Adds a spec file to make this facet available for the Java client. Includes a README*
* Docs: add a TSC doc [`#1303`](https://github.com/OpenLineage/OpenLineage/pull/1303) [@merobi-hub](https://github.com/merobi-hub)  
    *Adds a document listing the members of the Technical Steering Committee.*

### Changed
* Spark: enable usage of other Transports via Spark configuration [`#1383`](https://github.com/OpenLineage/OpenLineage/pull/1383) [@tnazarew](https://github.com/tnazarew)
    * OL client argument parsing moved from Spark Integration to java client

### Fixed
* Spark: improve Databricks to send better events [`#1330`](https://github.com/OpenLineage/OpenLineage/pull/1330) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Filters unwanted events and provides a meaningful job name.*
* Spark-Bigquery: fix a few of the common errors [`#1377`](https://github.com/OpenLineage/OpenLineage/pull/1377) [@mobuchowski](https://github.com/mobuchowski)  
    *Fixes a few of the common issues with the Spark-Bigquery integration and adds an integration test and configures CI.*
* Python: validate `eventTime` field in Python client [`#1355`](https://github.com/OpenLineage/OpenLineage/pull/1355) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Validates the `eventTime` of a `RunEvent` within the client library.*
* Databricks: Handle Databricks Runtime 11.3 changes to `DbFsUtils` constructor [`#1351`](https://github.com/OpenLineage/OpenLineage/pull/1351) [@wjohnson](https://github.com/wjohnson)  
    *Recaptures lost mount point information from the `DatabricksEnvironmentFacetBuilder` and environment-properties facet by looking at the number of parameters in the `DbFsUtils` constructor to determine the runtime version.*

## [0.17.0](https://github.com/OpenLineage/OpenLineage/compare/0.16.1...0.17.0) - 2022-11-16
### Added
* Spark: support latest Spark 3.3.1 [`#1183`](https://github.com/OpenLineage/OpenLineage/pull/1183) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for the latest Spark 3.3.1 version.*
* Spark: add Kinesis Transport and support config Kinesis in Spark integration [`#1200`](https://github.com/OpenLineage/OpenLineage/pull/1200) [@yogayang](https://github.com/yogyang)  
    *Adds support for sending to Kinesis from the Spark integration.* 
* Spark: Disable specified facets [`#1271`](https://github.com/OpenLineage/OpenLineage/pull/1271) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds the ability to disable specified facets from generated OpenLineage events.*
* Python: add facets implementation to Python client [`#1233`](https://github.com/OpenLineage/OpenLineage/pull/1233) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds missing facets to the Python client.*
* SQL: add Rust parser interface [`#1172`](https://github.com/OpenLineage/OpenLineage/pull/1172) [@StarostaGit](https://github.com/StarostaGit) [@mobuchowski](https://github.com/mobuchowski)  
    *Implements a Java interface in the Rust SQL parser, including a build script, native library loading mechanism, CI support and build fixes.*
* Proxy: add helm chart for the proxy backed [`#1068`](https://github.com/OpenLineage/OpenLineage/pull/1068) [@wslulciuc](https://github.com/wslulciuc)  
    *Adds a helm chart for deploying the proxy backend on Kubernetes.*
* Spec: include possible facets usage in spec [`#1249`](https://github.com/OpenLineage/OpenLineage/pull/1249) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Extends the `facets` definition with a list of available facets.*
* Website: publish YML version of spec to website [`#1300`](https://github.com/OpenLineage/OpenLineage/pull/1300) [@rossturk](https://github.com/rossturk)  
    *Adds configuration necessary to make the OpenLineage website auto-generate openAPI docs when the spec is published there.*
* Docs: update language on nominating new committers [`#1270`](https://github.com/OpenLineage/OpenLineage/pull/1270) [@rossturk](https://github.com/rossturk)  
    *Updates the governance language to reflect the new policy on nominating committers.*

### Changed
* Website: publish spec into new website repo location [`#1295`](https://github.com/OpenLineage/OpenLineage/pull/1295) [@rossturk](https://github.com/rossturk)  
    *Creates a new deploy key, adds it to CircleCI & GitHub, and makes the necessary changes to the `release.sh` script.*
* Airflow: change how pip installs packages in tox environments [`#1302`](https://github.com/OpenLineage/OpenLineage/pull/1302) [@JDarDagran](https://github.com/JDarDagran)  
    *Use deprecated resolver and constraints files provided by Airflow to avoid potential issues caused by pip's new resolver.*

### Fixed
* Airflow: fix README for running integration test [`#1238`](https://github.com/OpenLineage/OpenLineage/pull/1238) [@sekikn](https://github.com/sekikn)  
    *Updates the README for consistency with supported Airflow versions.*
* Airflow: add `task_instance` argument to `get_openlineage_facets_on_complete` [`#1269`](https://github.com/OpenLineage/OpenLineage/pull/1269) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds the `task_instance` argument to `DefaultExtractor`.*
* Java client: fix up all artifactory paths [`#1290`](https://github.com/OpenLineage/OpenLineage/pull/1290) [@harels](https://github.com/harels)  
    *Not all artifactory paths were changed in the build CI script in a previous PR.*
* Python client: fix Mypy errors and adjust to PEP 484 [`#1264`](https://github.com/OpenLineage/OpenLineage/pull/1264) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds a `--no-namespace-packages` argument to the Mypy command and adjusts code to PEP 484.*
* Website: release all specs since `last_spec_commit_id`, not just HEAD~1 [`#1298`](https://github.com/OpenLineage/OpenLineage/pull/1298) [@rossturk](https://github.com/rossturk)  
    *The script now ships all specs that have changed since `.last_spec_commit_id`.*

### Removed
* Deprecate HttpTransport.Builder in favor of HttpConfig [`#1287`](https://github.com/OpenLineage/OpenLineage/pull/1287) [@collado-mike](https://github.com/collado-mike)  
    *Deprecates the Builder in favor of HttpConfig only and replaces the existing Builder implementation by delegating to the HttpConfig.*

## [0.16.1](https://github.com/OpenLineage/OpenLineage/compare/0.15.1...0.16.1) - 2022-11-3
### Added
* Airflow: add `dag_run` information to Airflow version run facet [`#1133`](https://github.com/OpenLineage/OpenLineage/pull/1133) [@fm100](https://github.com/fm100)  
    *Adds the Airflow DAG run ID to the `taskInfo` facet, making this additional information available to the integration.*
* Airflow: add `LoggingMixin` to extractors [`#1149`](https://github.com/OpenLineage/OpenLineage/pull/1149) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds a `LoggingMixin` class to the custom extractor to make the output consistent with general Airflow and OpenLineage logging settings.*
* Airflow: add default extractor [`#1162`](https://github.com/OpenLineage/OpenLineage/pull/1162) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds a `DefaultExtractor` to support the default implementation of OpenLineage for external operators without the need for custom extractors.*
* Airflow: add `on_complete` argument in `DefaultExtractor` [`#1188`](https://github.com/OpenLineage/OpenLineage/pull/1188) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds support for running another method on `extract_on_complete`.*
* SQL: reorganize the library into multiple packages [`#1167`](https://github.com/OpenLineage/OpenLineage/pull/1167) [@StarostaGit](https://github.com/StarostaGit) [@mobuchowski](https://github.com/mobuchowski)   
    *Splits the SQL library into a Rust implementation and foreign language bindings, easing the process of adding language interfaces. Also contains CI fix.*

### Changed
* Airflow: move `get_connection_uri` as extractor's classmethod [`#1169`](https://github.com/OpenLineage/OpenLineage/pull/1169) [@JDarDagran](https://github.com/JDarDagran)  
    *The `get_connection_uri` method allowed for too many params, resulting in unnecessarily long URIs. This changes the logic to whitelisting per extractor.*
* Airflow: change `get_openlineage_facets_on_start/complete` behavior [`#1201`](https://github.com/OpenLineage/OpenLineage/pull/1201) [@JDarDagran](https://github.com/JDarDagran)  
    *Splits up the method for greater legibility and easier maintenance.*

### Fixed
* Airflow: always send SQL in `SqlJobFacet` as a string [`#1143`](https://github.com/OpenLineage/OpenLineage/pull/1143) [@mobuchowski](https://github.com/mobuchowski)  
    *Changes the data type of `query` from array to string to an fix error in the `RedshiftSQLOperator`.* 
* Airflow: include `__extra__` case when filtering URI query params [`#1144`](https://github.com/OpenLineage/OpenLineage/pull/1144) [@JDarDagran](https://github.com/JDarDagran)  
    *Includes the `conn.EXTRA_KEY` in the `get_connection_uri` method to avoid exposing secrets in URIs via the `__extra__` key.*  
* Airflow: enforce column casing in `SQLCheckExtractor`s [`#1159`](https://github.com/OpenLineage/OpenLineage/pull/1159) [@denimalpaca](https://github.com/denimalpaca)  
    *Uses the parent extractor's `_is_uppercase_names` property to determine if the column should be upper cased in the `SQLColumnCheckExtractor`'s `_get_input_facets()` method.*
* Spark: prevent exception when no schema provided [`#1180`](https://github.com/OpenLineage/OpenLineage/pull/1180) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Prevents evalution of column lineage when the `schemFacet` is `null`.*
* Great Expectations: add V3 API compatibility [`#1194`](https://github.com/OpenLineage/OpenLineage/pull/1194) [@denimalpaca](https://github.com/denimalpaca)  
    *Fixes the Pandas datasource to make it V3 API-compatible.*

### Removed
* Airflow: remove support for Airflow 1.10 [`#1128`](https://github.com/OpenLineage/OpenLineage/pull/1128) [@mobuchowski](https://github.com/mobuchowski)  
    *Removes the code structures and tests enabling support for Airflow 1.10.*

## [0.15.1](https://github.com/OpenLineage/OpenLineage/compare/0.14.1...0.15.1) - 2022-10-05
### Added
* Airflow: improve development experience [`#1101`](https://github.com/OpenLineage/OpenLineage/pull/1101) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds an interactive development environment to the Airflow integration and improves integration testing.*
* Spark: add description for URL parameters in readme, change `overwriteName` to `appName` [`#1130`](https://github.com/OpenLineage/OpenLineage/pull/1130) [@tnazarew](https://github.com/tnazarew)  
    *Adds more information about passing arguments with `spark.openlineage.url` and changes `overwriteName` to `appName` for clarity.*
* Documentation: update issue templates for proposal & add new integration template [`#1116`](https://github.com/OpenLineage/OpenLineage/pull/1116) [@rossturk](https://github.com/rossturk)  
    *Adds a YAML issue template for new integrations and fixes a bug in the proposal template.*

### Changed
* Airflow: lazy load BigQuery client [`#1119`](https://github.com/OpenLineage/OpenLineage/pull/1119) [@mobuchowski](https://github.com/mobuchowski)  
    *Moves import of the BigQuery client from top level to local level to decrease DAG import time.*

### Fixed
* Airflow: fix UUID generation conflict for Airflow DAGs with same name [`#1056`](https://github.com/OpenLineage/OpenLineage/pull/1056) [@collado-mike](https://github.com/collado-mike)  
    *Adds a namespace to the UUID calculation to avoid conflicts caused by DAGs having the same name in different namespaces in Airflow deployments.*
* Spark/BigQuery: fix issue with spark-bigquery-connector >=0.25.0 [`#1111`](https://github.com/OpenLineage/OpenLineage/pull/1111) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Makes the Spark integration compatible with the latest connector.*
* Spark: fix column lineage [`#1069`](https://github.com/OpenLineage/OpenLineage/pull/1069) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes a null pointer exception error and an error when `openlineage.timeout` is not provided.*
* Spark: set log level of `Init OpenLineageContext` to DEBUG [`#1064`](https://github.com/OpenLineage/OpenLineage/pull/1064) [@varuntestaz](https://github.com/varuntestaz)  
    *Prevents sensitive information from being logged unless debug mode is used.*
* Java client: update version of SnakeYAML [`#1090`](https://github.com/OpenLineage/OpenLineage/pull/1090) [@TheSpeedding](https://github.com/TheSpeedding)  
    *Bumps the SnakeYAML library version to include a key bug fix.* 
* dbt: remove requirement for `OPENLINEAGE_URL` to be set [`#1107`](https://github.com/OpenLineage/OpenLineage/pull/1107) [@mobuchowski](https://github.com/mobuchowski)  
    *Removes erroneous check for `OPENLINEAGE_URL` in the dbt integration.*
* Python client: remove potentially cyclic import [`#1126`](https://github.com/OpenLineage/OpenLineage/pull/1126) [@mobuchowski](https://github.com/mobuchowski)  
    *Hides imports to remove potentially cyclic import.*
* CI: build macos release package on medium resource class [`#1131`](https://github.com/OpenLineage/OpenLineage/pull/1131) [@mobuchowski](https://github.com/mobuchowski)  
    *Fixes failing build due to resource class being too large.*

## [0.14.1](https://github.com/OpenLineage/OpenLineage/compare/0.14.0...0.14.1) - 2022-09-07
### Fixed
* Fix Spark integration issues including error when no `openlineage.timeout` [`#1069`](https://github.com/OpenLineage/OpenLineage/pull/1069) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *`OpenlineageSparkListener` was failing when no `openlineage.timeout` was provided.* 

## [0.14.0](https://github.com/OpenLineage/OpenLineage/compare/0.13.1...0.14.0) - 2022-09-06
### Added
* Support ABFSS and Hadoop Logical Relation in Column-level lineage [`#1008`](https://github.com/OpenLineage/OpenLineage/pull/1008) [@wjohnson](https://github.com/wjohnson)  
    *Introduces an `extractDatasetIdentifier` that uses similar logic to `InsertIntoHadoopFsRelationVisitor` to pull out the path on the HDFS compliant file system; tested on ABFSS and DBFS (Databricks FileSystem) to prove that lineage could be extracted using non-SQL commands.*
* Add Kusto relation visitor [`#939`](https://github.com/OpenLineage/OpenLineage/pull/939) [@hmoazam](https://github.com/hmoazam)  
    *Implements a `KustoRelationVisitor` to support lineage for Azure Kusto's Spark connector.*
* Add ColumnLevelLineage facet doc [`#1020`](https://github.com/OpenLineage/OpenLineage/pull/1020) [@julienledem](https://github.com/julienledem)  
    *Adds documentation for the Column-level lineage facet.*
* Include symlinks dataset facet [`#935`](https://github.com/OpenLineage/OpenLineage/pull/935) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Includes the recently introduced `SymlinkDatasetFacet` in generated OpenLineage events.*
* Add support for dbt 1.3 beta's metadata changes [`#1051`](https://github.com/OpenLineage/OpenLineage/pull/1051) [@mobuchowski](https://github.com/mobuchowski)  
    *Makes projects that are composed of only SQL models work on 1.3 beta (dbt 1.3 renamed the `compiled_sql` field to `compiled_code` to support Python models). Does not provide support for dbt's Python models.*
* Support Flink 1.15 [`#1009`](https://github.com/OpenLineage/OpenLineage/pull/1009) [@mzareba382](https://github.com/mzareba382)  
    *Adds support for Flink 1.15.*
* Add Redshift dialect to the SQL integration [`#1066`](https://github.com/OpenLineage/OpenLineage/pull/1066) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds support for Redshift's SQL dialect in OpenLineage's SQL parser, including quirks such as the use of square brackets in JSON paths. (Note, this does not add support for all of Redshift's custom syntax.)*

### Changed
* Make the timeout configurable in the Spark integration [`#1050`](https://github.com/OpenLineage/OpenLineage/pull/1050) [@tnazarew](https://github.com/tnazarew)  
    *Makes timeout configurable by the user. (In some cases, the time needed to send events was longer than 5 seconds, which exceeded the timeout value.)*

### Fixed
* Add a dialect parameter to Great Expectations SQL parser calls [`#1049`](https://github.com/OpenLineage/OpenLineage/pull/1049) [@collado-mike](https://github.com/collado-mike)  
    *Specifies the dialect name from the SQL engine.*
* Fix Delta 2.1.0 with Spark 3.3.0 [`#1065`](https://github.com/OpenLineage/OpenLineage/pull/1065) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Allows delta support for Spark 3.3 and fixes potential issues. (The Openlineage integration for Spark 3.3 was turned on without delta support, as delta did not support Spark 3.3 at that time.)*

## [0.13.1](https://github.com/OpenLineage/OpenLineage/compare/0.13.0...0.13.1) - 2022-08-25
### Fixed
* Rename all `parentRun` occurrences to `parent` in Airflow integration [`1037`](https://github.com/OpenLineage/OpenLineage/pull/1037) [@fm100](https://github.com/fm100)  
    *Changes the `parentRun` property name to `parent` in the Airflow integration to match the spec.*
* Do not change task instance during `on_running` event [`1028`](https://github.com/OpenLineage/OpenLineage/pull/1028) [@JDarDagran](https://github.com/JDarDagran)  
    *Fixes an issue in the Airflow integration with the `on_running` hook, which was changing the `TaskInstance` object along with the `task` attribute.*

## [0.13.0](https://github.com/OpenLineage/OpenLineage/compare/0.12.0...0.13.0) - 2022-08-22
### Added

* Add BigQuery check support [`#960`](https://github.com/OpenLineage/OpenLineage/pull/960) [@denimalpaca](https://github.com/denimalpaca)  
    *Adds logic and support for proper dynamic class inheritance for BigQuery-style operators. (BigQuery's extractor needed additional logic to support the forthcoming `BigQueryColumnCheckOperator` and `BigQueryTableCheckOperator`.)*
* Add `RUNNING` `EventType` in spec and Python client [`#972`](https://github.com/OpenLineage/OpenLineage/pull/972) [@mzareba382](https://github.com/mzareba382)  
    *Introduces a `RUNNING` event state in the OpenLineage spec to indicate a running task and adds a `RUNNING` event type in the Python API.*
* Use databases & schemas in SQL Extractors [`#974`](https://github.com/OpenLineage/OpenLineage/pull/974) [@JDarDagran](https://github.com/JDarDagran)  
    *Allows the Airflow integration to differentiate between databases and schemas. (There was no notion of databases and schemas when querying and parsing results from `information_schema` tables.)*
* Implement Event forwarding feature via HTTP protocol [`#995`](https://github.com/OpenLineage/OpenLineage/pull/995) [@howardyoo](https://github.com/howardyoo)  
    *Adds `HttpLineageStream` to forward a given OpenLineage event to any HTTP endpoint.*
* Introduce `SymlinksDatasetFacet` to spec [`#936`](https://github.com/OpenLineage/OpenLineage/pull/936) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Creates a new facet, the `SymlinksDatasetFacet`, to support the storing of alternative dataset names.*
* Add Azure Cosmos Handler to Spark integration [`#983`](https://github.com/OpenLineage/OpenLineage/pull/983) [@hmoazam](https://github.com/hmoazam)  
    *Defines a new interface, the `RelationHandler`, to support Spark data sources that do not have `TableCatalog`, `Identifier`, or `TableProperties` set, as is the case with the Azure Cosmos DB Spark connector.*
* Support OL Datasets in manual lineage inputs/outputs [`#1015`](https://github.com/OpenLineage/OpenLineage/pull/1015) [@conorbev](https://github.com/conorbev)  
    *Allows Airflow users to create OpenLineage Dataset classes directly in DAGs with no conversion necessary. (Manual lineage definition required users to create an `airflow.lineage.entities.Table`, which was then converted to an OpenLineage Dataset.)* 
* Create ownership facets [`#996`](https://github.com/OpenLineage/OpenLineage/pull/996) [@julienledem](https://github.com/julienledem)  
    *Adds an ownership facet to both Dataset and Job in the OpenLineage spec to capture ownership of jobs and datasets.*

### Changed
* Use `RUNNING` EventType in Flink integration for currently running jobs [`#985`](https://github.com/OpenLineage/OpenLineage/pull/985) [@mzareba382](https://github.com/mzareba382)  
    *Makes use of the new `RUNNING` event type in the Flink integration, changing events sent by Flink jobs from `OTHER` to this new type.*
* Convert task objects to JSON-encodable objects when creating custom Airflow version facets [`#1018`](https://github.com/OpenLineage/OpenLineage/pull/1018) [@fm100](https://github.com/fm100)  
    *Implements a `to_json_encodable` function in the Airflow integration to make task objects JSON-encodable.*

### Fixed
* Add support for custom SQL queries in v3 Great Expectations API [`#1025`](https://github.com/OpenLineage/OpenLineage/pull/1025) [@collado-mike](https://github.com/collado-mike)  
    *Fixes support for custom SQL statements in the Great Expectations provider. (The Great Expectations custom SQL datasource was not applied to the support for the V3 checkpoints API.)*
    
## [0.12.0](https://github.com/OpenLineage/OpenLineage/compare/0.11.0...0.12.0) - 2022-08-01
### Added

* Add Spark 3.3.0 support [`#950`](https://github.com/OpenLineage/OpenLineage/pull/950) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Add Apache Flink integration [`#951`](https://github.com/OpenLineage/OpenLineage/pull/951) [@mobuchowski](https://github.com/mobuchowski)
* Add ability to extend column level lineage mechanism [`#922`](https://github.com/OpenLineage/OpenLineage/pull/922) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Add ErrorMessageRunFacet [`#897`](https://github.com/OpenLineage/OpenLineage/pull/897) [@mobuchowski](https://github.com/mobuchowski)
* Add SQLCheckExtractors [`#717`](https://github.com/OpenLineage/OpenLineage/pull/717) [@denimalpaca](https://github.com/denimalpaca)
* Add RedshiftSQLExtractor & RedshiftDataExtractor [`#930`](https://github.com/OpenLineage/OpenLineage/pull/930) [@JDarDagran](https://github.com/JDarDagran)
* Add dataset builder for AlterTableCommand [`#927`](https://github.com/OpenLineage/OpenLineage/pull/927) [@tnazarew](https://github.com/tnazarew)

### Changed

* Limit Delta events [`#905`](https://github.com/OpenLineage/OpenLineage/pull/905) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Airflow integration: allow lineage metadata to flow through inlets and outlets [`#914`](https://github.com/OpenLineage/OpenLineage/pull/914) [@fenil25](https://github.com/fenil25)

### Fixed

* Limit size of serialized plan [`#917`](https://github.com/OpenLineage/OpenLineage/pull/917) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Fix noclassdef error [`#942`](https://github.com/OpenLineage/OpenLineage/pull/942) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)

## [0.11.0](https://github.com/OpenLineage/OpenLineage/compare/0.10.0...0.11.0) - 2022-07-07
### Added

* HTTP option to override timeout and properly close connections in `openlineage-java` lib. [`#909`](https://github.com/OpenLineage/OpenLineage/pull/909) [@mobuchowski](https://github.com/mobuchowski)
* Dynamic mapped tasks support to Airflow integration [`#906`](https://github.com/OpenLineage/OpenLineage/pull/906) [@JDarDagran](https://github.com/JDarDagran)
* `SqlExtractor` to Airflow integration [`#907`](https://github.com/OpenLineage/OpenLineage/pull/907) [@JDarDagran](https://github.com/JDarDagran)
* [PMD](https://pmd.github.io) to Java and Spark builds in CI [`#898`](https://github.com/OpenLineage/OpenLineage/pull/898) [@merobi-hub](https://github.com/merobi-hub)

### Changed

* When testing extractors in the Airflow integration, set the extractor length assertion dynamic [`#882`](https://github.com/OpenLineage/OpenLineage/pull/882) [@denimalpaca](https://github.com/denimalpaca)
* Render templates as start of integration tests for `TaskListener` in the Airflow integration [`#870`](https://github.com/OpenLineage/OpenLineage/pull/870) [@mobuchowski](https://github.com/mobuchowski) 

### Fixed

* Dependencies bundled with `openlineage-java` lib. [`#855`](https://github.com/OpenLineage/OpenLineage/pull/855) [@collado-mike](https://github.com/collado-mike)
* [PMD](https://pmd.github.io) reported issues [`#891`](https://github.com/OpenLineage/OpenLineage/pull/891) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Spark casting error and session catalog support for `iceberg` in Spark integration [`#856`](https://github.com/OpenLineage/OpenLineage/pull/856) [@wslulciuc](https://github.com/wslulciuc)

## [0.10.0](https://github.com/OpenLineage/OpenLineage/compare/0.9.0...0.10.0) - 2022-06-24
### Added

* Add static code anlalysis tool [mypy](http://mypy-lang.org) to run in CI for against all python modules ([`#802`](https://github.com/openlineage/openlineage/issues/802)) [@howardyoo](https://github.com/howardyoo)
* Extend `SaveIntoDataSourceCommandVisitor` to extract schema from `LocalRelaiton` and `LogicalRdd` in spark integration ([`#794`](https://github.com/OpenLineage/OpenLineage/pull/794)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Add `InMemoryRelationInputDatasetBuilder` for `InMemory` datasets to Spark integration ([`#818`](https://github.com/OpenLineage/OpenLineage/pull/818)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Add copyright to source files [`#755`](https://github.com/OpenLineage/OpenLineage/pull/755) [@merobi-hub](https://github.com/merobi-hub)
* Add `SnowflakeOperatorAsync` extractor support to Airflow integration [`#869`](https://github.com/OpenLineage/OpenLineage/pull/869) [@merobi-hub](https://github.com/merobi-hub)
* Add PMD analysis to proxy project ([`#889`](https://github.com/OpenLineage/OpenLineage/pull/889)) [@howardyoo](https://github.com/howardyoo)

### Changed

* Skip `FunctionRegistry.class` serialization in Spark integration ([`#828`](https://github.com/OpenLineage/OpenLineage/pull/828)) [@mobuchowski](https://github.com/mobuchowski)
* Install new `rust`-based SQL parser by default in Airflow integration ([`#835`](https://github.com/OpenLineage/OpenLineage/pull/835)) [@mobuchowski](https://github.com/mobuchowski)
* Improve overall `pytest` and integration tests for Airflow integration ([`#851`](https://github.com/OpenLineage/OpenLineage/pull/851),[`#858`](https://github.com/OpenLineage/OpenLineage/pull/858)) [@denimalpaca](https://github.com/denimalpaca)
* Reduce OL event payload size by excluding local data and including output node in start events ([`#881`](https://github.com/OpenLineage/OpenLineage/pull/881)) [@collado-mike](https://github.com/collado-mike)
* Split spark integration into submodules ([`#834`](https://github.com/OpenLineage/OpenLineage/pull/834), [`#890`](https://github.com/OpenLineage/OpenLineage/pull/890)) [@tnazarew](https://github.com/tnazarew) [@mobuchowski](https://github.com/mobuchowski)

### Fixed

* Conditionally import `sqlalchemy` lib for Great Expectations integration ([`#826`](https://github.com/OpenLineage/OpenLineage/pull/826)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Add check for missing **class** `org.apache.spark.sql.catalyst.plans.logical.CreateV2Table` in Spark integration ([`#866`](https://github.com/OpenLineage/OpenLineage/pull/866)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Fix static code analysis issues ([`#867`](https://github.com/OpenLineage/OpenLineage/pull/867),[`#874`](https://github.com/OpenLineage/OpenLineage/pull/874)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)

## [0.9.0](https://github.com/OpenLineage/OpenLineage/compare/0.8.2...0.9.0)
### Added
* Spark: Column-level lineage introduced for Spark integration ([#698](https://github.com/OpenLineage/OpenLineage/pull/698), [#645](https://github.com/OpenLineage/OpenLineage/pull/645)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Java: Spark to use Java client directly ([#774](https://github.com/OpenLineage/OpenLineage/pull/774)) [@mobuchowski](https://github.com/mobuchowski)
* Clients: Add OPENLINEAGE_DISABLED environment variable which overrides config to NoopTransport ([#780](https://github.com/OpenLineage/OpenLineage/pull/780)) [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* Set log to debug on unknown facet entry ([#766](https://github.com/OpenLineage/OpenLineage/pull/766)) [@wslulciuc](https://github.com/wslulciuc)
* Dagster: pin protobuf version to 3.20 as suggested by tests ([#787](https://github.com/OpenLineage/OpenLineage/pull/787)) [@mobuchowski](https://github.com/mobuchowski)
* Add SafeStrDict to skip failing attibutes ([#798](https://github.com/OpenLineage/OpenLineage/pull/798)) [@JDarDagran](https://github.com/JDarDagran)

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
