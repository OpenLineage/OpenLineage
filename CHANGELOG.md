# Changelog

## [Unreleased](https://github.com/OpenLineage/OpenLineage/compare/1.20.4...HEAD)

## [1.20.4](https://github.com/OpenLineage/OpenLineage/compare/1.19.0...1.20.4) - 2024-08-22

### Added
* **Python: add `CompositeTransport`** [`#2925`](https://github.com/OpenLineage/OpenLineage/pull/2925) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds a `CompositeTransport` that can accept other transport configs to instantiate transports and use them to emit events.*
* **Spark: compile & test Spark integration on Java 17** [`#2828`](https://github.com/OpenLineage/OpenLineage/pull/2828) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *The Spark integration is always compiled with Java 17, while tests are running on both Java 8 and Java 17 according to the configuration.*
* **Spark: support preview release of Spark 4.0** [`#2854`](https://github.com/OpenLineage/OpenLineage/pull/2854) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Includes the Spark 4.0 preview release in the integration tests.*
* **Spark: add handling for `Window`** [`#2901`](https://github.com/OpenLineage/OpenLineage/pull/2901) [@tnazarew](https://github.com/tnazarew)  
    *Adds handling for `Window`-type nodes of a logical plan.*
* **Spark: extract and send events with raw SQL from Spark** [`#2913`](https://github.com/OpenLineage/OpenLineage/pull/2913) [@Imbruced](https://github.com/Imbruced)  
    *Adds a parser that traverses `QueryExecution` to get the SQL query used from the SQL field with a BFS algorithm.*
* **Spark: support Mongostream source** [`#2887`](https://github.com/OpenLineage/OpenLineage/pull/2887) [@Imbruced](https://github.com/Imbruced)  
    *Adds a Mongo streaming visitor and tests.*
* **Spark: new mechanism for disabling facets** [`#2912`](https://github.com/OpenLineage/OpenLineage/pull/2912) [@arturowczarek](https://github.com/arturowczarek)  
    *The mechanism makes `FacetConfig` accept the disabled flag for any facet instead of passing them as a list.*
* **Spark: support Kinesis source** [`#2906`](https://github.com/OpenLineage/OpenLineage/pull/2906) [@Imbruced](https://github.com/Imbruced)  
    *Adds a Kinesis class handler in the streaming source builder.*
* **Spark: extract `DatasetIdentifier` from extension `LineageNode`** [`#2900`](https://github.com/OpenLineage/OpenLineage/pull/2900) [@ddebowczyk92](https://github.com/ddebowczyk92)  
    *Adds support for cases in which `LogicalRelation` has a grandChild node that implements the `LineageRelation` interface.*
* **Spark: extract Dataset from underlying `BaseRelation`** [`#2893`](https://github.com/OpenLineage/OpenLineage/pull/2893) [@ddebowczyk92](https://github.com/ddebowczyk92)  
    *`DatasetIdentifier` is now extracted from the underlying node of `LogicalRelation`.*
* **Spark: add descriptions and Marquez UI to Docker Compose file** [`#2889`](https://github.com/OpenLineage/OpenLineage/pull/2889) [@jonathanlbt1](https://github.com/jonathanlbt1)  
    *Adds the `marquez-web` service to docker-compose.yml.*

### Fixed
* **Proxy: bug fixed on error messages descriptions** [`#2880`](https://github.com/OpenLineage/OpenLineage/pull/2880) [@jonathanlbt1](https://github.com/jonathanlbt1)  
    *Improves error logging.*
* **Proxy: update Docker image for Fluentd 1.17** [`#2877`](https://github.com/OpenLineage/OpenLineage/pull/2877) [@jonathanlbt1](https://github.com/jonathanlbt1)  
    *Upgrades the Fluentd version.*
* **Spark: fix issue with Kafka source when saving with `for each` batch method** [`#2868`](https://github.com/OpenLineage/OpenLineage/pull/2868) [@imbruced](https://github.com/Imbruced)  
    *Fixes an issue when Spark is in streaming mode and input for Kafka was not present in the event.*
* **Spark: properly set ARN in namespace for Iceberg Glue symlinks** [`#2943`](https://github.com/OpenLineage/OpenLineage/pull/2943) [@arturowczarek](https://github.com/arturowczarek)  
    *Makes `IcebergHandler` support Glue catalog tables and create the symlink using the code from `PathUtils`.*
* **Spark: accept any provider for AWS Glue storage format** [`#2917`](https://github.com/OpenLineage/OpenLineage/pull/2917) [@arturowczarek](https://github.com/arturowczarek)  
    *Makes the AWS Glue ARN generating method accept every format (including Parquet), not only Hive SerDe.*
* **Spark: return valid JSON for failed logical plan serialization** [`#2892`](https://github.com/OpenLineage/OpenLineage/pull/2892) [@arturowczarek](https://github.com/arturowczarek)  
    *The `LogicalPlanSerializer` now returns `<failed-to-serialize-logical-plan>` for failed serialization instead of an empty string.*
* **Spark: extract legacy column lineage visitors loader** [`#2883`](https://github.com/OpenLineage/OpenLineage/pull/2883) [@arturowczarek](https://github.com/arturowczarek)  
    *Refactors `CustomCollectorsUtils` for improved readability.*
* **Spark: add Kafka input source when writing in `foreach` batch mode** [`#2868`](https://github.com/OpenLineage/OpenLineage/pull/2868) [@Imbruced](https://github.com/Imbruced)  
    *Fixes a bug keeping Kafka input sources from being produced.*
* **Spark: extract `DatasetIdentifier` from `SaveIntoDataSourceCommandVisitor` options** [`#2934`](https://github.com/OpenLineage/OpenLineage/pull/2934) [@ddebowczyk92](https://github.com/ddebowczyk92)  
    *Extracts `DatasetIdentifier` from command's options instead of relying on `p.createRelation(sqlContext, command.options())`, which is a heavy operation for `JdbcRelationProvider`.*

## [1.19.0](https://github.com/OpenLineage/OpenLineage/compare/1.18.0...1.19.0) - 2024-07-22

### Added
* **Airflow: add `log_url` to `AirflowRunFacet`** [`#2852`](https://github.com/OpenLineage/OpenLineage/pull/2852) [@dolfinus](https://github.com/dolfinus)  
    *Adds taskinstance's `log_url` field to `AirflowRunFacet`*
* **Spark: add handling for `Generate`** [`#2856`](https://github.com/OpenLineage/OpenLineage/pull/2856) [@tnazarew](https://github.com/tnazarew)  
    *Adds handling for `Generate`-type nodes of a logical plan (e.g., explode operations).*
* **Java: add `DerbyJdbcExtractor`** [`#2869`](https://github.com/OpenLineage/OpenLineage/pull/2869) [@dolfinus](https://github.com/dolfinus)  
    *Adds `JdbcExtractor` implementation for Derby database. As this is a file-based DBMS, its Dataset namespace is `file` and name is an absolute path to a database file.*
* **Spark: verify bytecode version of the built jar** [`#2859`](https://github.com/OpenLineage/OpenLineage/pull/2859) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Extends the `JarVerifier` plugin to ensure all compiled classes have a bytecode version of Java 8 or lower.*
* **Spark: add Kafka streaming source support** [`#2851`](https://github.com/OpenLineage/OpenLineage/pull/2851) [@d-m-h](https://github.com/d-m-h) [@imbruced](https://github.com/Imbruced)  
    *Adds support for Kafka streaming sources to Kafka streaming sinks. Inputs and outputs are now included in lineage events.*

### Fixed
* **Airflow: replace datetime.now with airflow.utils.timezone.utcnow** [`#2865`](https://github.com/OpenLineage/OpenLineage/pull/2865) [@kacpermuda](https://github.com/kacpermuda)  
    *Fixes missing timezone information in task FAIL events*
* **Spark: remove shaded dependency in `ColumnLevelLineageBuilder`** [`#2850`](https://github.com/OpenLineage/OpenLineage/pull/2850) [@tnazarew](https://github.com/tnazarew)  
    *Removes the shaded `Streams` dependency in `ColumnLevelLineageBuilder` causing a `ClassNotFoundException`.*
* **Spark: make Delta dataset symlink consistent with non-Delta tables** [`#2863`](https://github.com/OpenLineage/OpenLineage/pull/2863) [@dolfinus](https://github.com/dolfinus)  
    *Makes dataset symlinks for Delta and non-Delta tables consistent.*
* **Spark: use Table's properties during column-level lineage construction** [`#2855`](https://github.com/OpenLineage/OpenLineage/pull/2855) [@ddebowczyk92](https://github.com/ddebowczyk92)  
    *Fixes `PlanUtils3` so Dataset identifier information based on a Table's properties is also retrieved during the construction of column-level lineage.*
* **Spark: extract job name creation to providers** [`#2861`](https://github.com/OpenLineage/OpenLineage/pull/2861) [@arturowczarek](https://github.com/arturowczarek)  
    *The integration now detects if the `spark.app.name` was autogenerated by Glue and uses the Glue job name in such cases. Also, each job name provisioning strategy is now extracted to a separate provider.*

## [1.18.0](https://github.com/OpenLineage/OpenLineage/compare/1.17.1...1.18.0) - 2024-07-11
### Added
* **Spark: configurable integration test** [`#2755`](https://github.com/OpenLineage/OpenLineage/pull/2755) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Provides command line tool capable of running Spark integration tests that can be created without Java.*
* **Spark: OpenLineage Spark extension interfaces without runtime dependency hell** [`#2809`](https://github.com/OpenLineage/OpenLineage/pull/2809) [`#2837`](https://github.com/OpenLineage/OpenLineage/pull/2837) [@ddebowczyk92](https://github.com/ddebowczyk92)  
    *New Spark extension interfaces without runtime dependency hell. Includes a test to verify the integration is working properly.*
* **Spark: support latest versions 3.4.3 and 3.5.1.** [`#2743`](https://github.com/OpenLineage/OpenLineage/pull/2743) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Upgrades CI workflows to run tests against latest Spark versions: 3.4.2 -> 3.4.3 and 3.5.0 -> 3.5.1.*
* **Spark: add extraction of the masking property in column-level lineage** [`#2789`](https://github.com/OpenLineage/OpenLineage/pull/2789) [@tnazarew](https://github.com/tnazarew)  
    *Adds extraction of the masking property during collection of dependencies for `ColumnLineageDatasetFacet` creation.*
* **Spark: collect table name from `InsertIntoHadoopFsRelationCommand`** [`#2794`](https://github.com/OpenLineage/OpenLineage/pull/2794) [@dolfinus](https://github.com/dolfinus)  
    *Collects a table name for `INSERT INTO` command for tables created with `USING $fileFormat` syntax, like `USING orc`.*
* **Spark, Flink: add `PostgresJdbcExtractor`** [`#2806`](https://github.com/OpenLineage/OpenLineage/pull/2806) [@dolfinus](https://github.com/dolfinus)  
    *Adds the default `5432` port to Postgres namespaces.*
* **Spark, Flink: add `TeradataJdbcExtractor`** [`#2826`](https://github.com/OpenLineage/OpenLineage/pull/2826) [@dolfinus](https://github.com/dolfinus)  
    *Converts JDBC URLs like `jdbc:teradata/host/DBS_PORT=1024,DATABASE=somedb` to datasets with namespace `teradata://host:1024` and name `somedb.table`.*
* **Spark, Flink: add `MySqlJdbcExtractor`** [`#2825`](https://github.com/OpenLineage/OpenLineage/pull/2825) [@dolfinus](https://github.com/dolfinus)  
    *Handles different formats of MySQL JDBC URL, and produces datasets with consistent namespaces, like `mysql://host:port`.*
* **Spark, Flink: add `OracleJdbcExtractor`** [`#2824`](https://github.com/OpenLineage/OpenLineage/pull/2824) [@dolfinus](https://github.com/dolfinus)  
    *Handles simple Oracle JDBC URLs, like `oracle:thin:@//host:port/serviceName` and `oracle:thin@host:port:sid`, and converts each to a dataset with namespace `oracle://host:port` and name `sid.schema.table` or `serviceName.schema.table`.*
* **Spark: configurable test with Docker image provided** [`#2822`](https://github.com/OpenLineage/OpenLineage/pull/2822) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Extends the configurable integration test feature to enable getting the Docker image name as a name.*
* **Spark: Support Iceberg 1.4 on Spark 3.5.1** [`#2838`](https://github.com/OpenLineage/OpenLineage/pull/2838) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Include Iceberg support for Spark 3.5. Fix column level lineage facet for `UNION` queries.*
* **Spec: add example for change in `#2756`** [`#2801`](https://github.com/OpenLineage/OpenLineage/pull/2801) [@Sheeri](https://github.com/Sheeri)  
    *Updates the `customLineage` facet test for the new syntax created in `#2756`.*

### Changed
* **Spark: fallback to `spark.sql.warehouse.dir` as table namespace** [`#2767`](https://github.com/OpenLineage/OpenLineage/pull/2767) [@dolfinus](https://github.com/dolfinus)  
    *In cases when a metastore is not used, falls back to `spark.sql.warehouse.dir` or `hive.metastore.warehouse.dir` as table namespace, instead of duplicating the table's location.*

### Fixed
* **Java: handle dashes in hostname for `JdbcExtractors`** [`#2830`](https://github.com/OpenLineage/OpenLineage/pull/2830) [@dolfinus](https://github.com/dolfinus)  
    *Proper handling of dashes in JDBC URL hosts.*
* **Spark: fix Glue symlinks formatting bug** [`#2807`](https://github.com/OpenLineage/OpenLineage/pull/2807) [@Akash2351](https://github.com/Akash2351)  
    *Fixes Glue symlinks with config parsing for Glue `catalogid`.*
* **Spark, Flink: fix DBFS namespace format** [`#2800`](https://github.com/OpenLineage/OpenLineage/pull/2800) [@dolfinus](https://github.com/dolfinus)  
    *Fixes the DBFS namespace format.*
* **Spark: fix Glue naming format** [`#2766`](https://github.com/OpenLineage/OpenLineage/pull/2766) [@dolfinus](https://github.com/dolfinus)  
    *Changes the AWS Glue namespace to match Glue ARN documentation.*
* **Spark: fix Iceberg dataset location** [`#2797`](https://github.com/OpenLineage/OpenLineage/pull/2797) [@dolfinus](https://github.com/dolfinus)  
    *Fixes Iceberg dataset namespace: instead of `file:/some/path/database.table` uses `file:/some/path/database/table`. For dataset TABLE symlink, uses warehouse location instead of database location.*
* **Spark: fix NPE and incorrect comment** [`#2827`](https://github.com/OpenLineage/OpenLineage/pull/2827) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes an error caused by a recent upgrade of Spark versions that did not break existing tests.*
* **Spark: convert scheme and authority to lowercase in `JdbcLocation`** [`#2831`](https://github.com/OpenLineage/OpenLineage/pull/2831) [@dolfinus](https://github.com/dolfinus)  
    *Converts valid JDBC URL scheme and authority to lowercase, leaving intact instance/database name, as different databases have different default case and case-sensitivity rules.*

## [1.17.1](https://github.com/OpenLineage/OpenLineage/compare/1.16.0...1.17.1) - 2024-06-21
### Added
* **Java: dataset namespace resolver feature** [`#2720`](https://github.com/OpenLineage/OpenLineage/pull/2720) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a dataset namespace resolving mechanism that resolves dataset namespaces based on the resolvers configured. The core mechanism is implemented in openlineage-java and can be used within the Flink and Spark integrations.*
* **Spark: add transformation extraction** [`#2758`](https://github.com/OpenLineage/OpenLineage/pull/2758) [@tnazarew](https://github.com/tnazarew)  
    *Adds a transformation type extraction mechanism.*
* **Spark: add GCP run and job facets** [`#2643`](https://github.com/OpenLineage/OpenLineage/pull/2643) [@codelixir](https://github.com/codelixir)  
    *Adds `GCPRunFacetBuilder` and `GCPJobFacetBuilder` to report additional facets when running on Google Cloud Platform.*
* **Spark: improve namespace format for SQLServer** [`#2773`](https://github.com/OpenLineage/OpenLineage/pull/2773) [@dolfinus](https://github.com/dolfinus)  
    *Improves the namespace format for SQLServer.*
* **Spark: verify jar content after build** [`#2698`](https://github.com/OpenLineage/OpenLineage/pull/2698) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a tool to verify `shadowJar` content and prevent reported issues. These are hard to prevent currently and require manual verification of manually unpacked jar content.*
* **Spec: add transformation type info** [`#2756`](https://github.com/OpenLineage/OpenLineage/pull/2756) [@tnazarew](https://github.com/tnazarew)  
    *Adds information about the transformation type in `ColumnLineageDatasetFacet`. `transformationType` and `transformationDescription` are marked as deprecated.*
* **Spec: implementing facet registry (following #2161)** [`#2729`](https://github.com/OpenLineage/OpenLineage/pull/2729) [@harels](https://github.com/harels)  
    *Introduces the foundations of the new facet Registry into the repo.*
* **Spec: register GCP common job facet** [`#2740`](https://github.com/OpenLineage/OpenLineage/pull/2740) [@ngorchakova](https://github.com/ngorchakova)  
    *Registers the GCP job facet that contains common attributes that will improve the way lineage is parsed and displayed by the GCP platform. Based on the [proposal](https://github.com/OpenLineage/OpenLineage/pull/2228/files), GCP Lineage would like to define facets that are expected from integrations. The list of support facets is not final and will be extended further by next PR.*

### Removed
* **Java: remove deprecated `localServerId` option from Kafka config** [`#2738`](https://github.com/OpenLineage/OpenLineage/pull/2738) [@dolfinus](https://github.com/dolfinus)  
    *Removes `localServerId` from Kafka config, deprecated since 1.13.0.*
* **Java: remove deprecated `Transport.emit(String)`** [`#2737`](https://github.com/OpenLineage/OpenLineage/pull/2737) [@dolfinus](https://github.com/dolfinus)  
    *Removes `Transport.emit(String)` support, deprecated since 1.13.0.*
* **Spark: remove `spark-interfaces-scala` module** [`#2781`](https://github.com/OpenLineage/OpenLineage/pull/2781)[@ddebowczyk92](https://github.com/ddebowczyk92)  
    *Replaces the existing `spark-interfaces-scala` interfaces with new ones decoupled from the Scala binary version. Allows for improved integration in environments where one cannot guarantee the same version of `openlineage-java`.*

### Changed
* **Spark: add log info when emitting lineage from Spark (following #2650)** [`#2769`](https://github.com/OpenLineage/OpenLineage/pull/2769) [@algorithmy1](https://github.com/algorithmy1)  
    *Enhances logging.*

### Fixed
* **Flink: use `namespace.name` as Avro complex field type** [`#2763`](https://github.com/OpenLineage/OpenLineage/pull/2763) [@dolfinus](https://github.com/dolfinus)  
    *`namespace.name` is now used as Avro `"type"` of complex fields (record, enum, fixed).*
* **Java: repair empty dataset name** [`#2776`](https://github.com/OpenLineage/OpenLineage/pull/2776) [@kacpermuda](https://github.com/kacpermuda)  
    *The dataset name should not be empty.*
* **Spark: fix events emitted for `drop table` for Spark 3.4 and above** [`#2745`](https://github.com/OpenLineage/OpenLineage/pull/2745) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)[@savannavalgi](https://github.com/savannavalgi)  
    *Includes dataset being dropped within the event, as it used to be prior to Spark 3.4.*
* **Spark, Flink: fix S3 dataset names** [`#2782`](https://github.com/OpenLineage/OpenLineage/pull/2782) [@dolfinus](https://github.com/dolfinus)  
    *Drops the leading slash from the object storage dataset name. Converts `s3a://` and `s3n://` schemes to `s3://`.*
* **Spark: fix Hive metastore namespace** [`#2761`](https://github.com/OpenLineage/OpenLineage/pull/2761) [@dolfinus](https://github.com/dolfinus)  
    *Fixes the dataset namespace for cases when the Hive metastore URL is set using `$SPARK_CONF_DIR/hive-site.xml`.*
* **Spark: fix NPE in column-level lineage** [`#2749`](https://github.com/OpenLineage/OpenLineage/pull/2749) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *The Spark agent now checks to determine if `cur.getDependencies()` is not null before adding dependencies.*
* **Spark: refactor `OpenLineageRunEventBuilder`** [`#2754`](https://github.com/OpenLineage/OpenLineage/pull/2754) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a separate class containing all the input arguments to call `OpenLineageRunEventBuilder::buildRun`.*
* **Spark: fix `historyUrl` format** [`#2741`](https://github.com/OpenLineage/OpenLineage/pull/2741) [@dolfinus](https://github.com/dolfinus)  
    *Fixes the `historyUrl` format in `spark_applicationDetails`.*
* **SQL: allow self-recursive aliases** [`#2753`](https://github.com/OpenLineage/OpenLineage/pull/2753) [@mobuchowski](https://github.com/mobuchowski)  
    *Expressions like `select * from test_orders as test_orders` are now parsed properly.*

## [1.16.0](https://github.com/OpenLineage/OpenLineage/compare/1.15.0...1.16.0) - 2024-05-28
### Added
* **Spark: add `jobType` facet to Spark application events** [`#2719`](https://github.com/OpenLineage/OpenLineage/pull/2719) [@dolfinus](https://github.com/dolfinus)  
    *Add `jobType` facet to `runEvent`s emitted by `SparkListenerApplicationStart`.*
* **Spark & Flink: Introduce dataset namespace resolver.** [`#2720`](https://github.com/OpenLineage/OpenLineage/pull/2720) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
  *Enable resolving dataset namespace with predefined resolvers like: `HostListNamespaceResolver`, `PatternNamespaceResolver`, `PatternMatchingGroupNamespaceResolver` or custom implementation loaded with ServiceLoader. Feature is useful to resolve hostnames into cluster identifiers.*

### Fixed
* **dbt: fix swapped namespace and name in dbt integration** [`#2735`](https://github.com/OpenLineage/OpenLineage/pull/2735) [@JDarDagran](https://github.com/JDarDagran)  
    *Fixes variable names.*
* **Python: override debug level** [`#2727`](https://github.com/OpenLineage/OpenLineage/pull/2735) [@mobuchowski](https://github.com/mobuchowski)  
    *Removes debug-level logging of HTTP requests.*

## [1.15.0](https://github.com/OpenLineage/OpenLineage/compare/1.14.0...1.15.0) - 2024-05-23
### Added
* **Flink: handle Iceberg tables with nested and complex field types** [`#2706`](https://github.com/OpenLineage/OpenLineage/pull/2706) [@dolfinus](https://github.com/dolfinus)  
    *Creates `SchemaDatasetFacet` with nested fields for Iceberg tables with list, map and struct columns.*
* **Flink: handle Avro schema with nested and complex field types** [`#2711`](https://github.com/OpenLineage/OpenLineage/pull/2711) [@dolfinus](https://github.com/dolfinus)  
    *Creates `SchemaDatasetFacet` with nested fields for Avro schemas with complex types (union, record, map, array, fixed).*
* **Spark: add facets to Spark application events** [`#2677`](https://github.com/OpenLineage/OpenLineage/pull/2677) [@dolfinus](https://github.com/dolfinus)  
    *Adds support for Spark application start and stop events in the `ExecutionContext` interface.*
* **Spark: add nested fields to `SchemaDatasetFieldsFacet`** [`#2689`](https://github.com/OpenLineage/OpenLineage/pull/2689) [@dolfinus](https://github.com/dolfinus)  
    *Adds nested Spark Dataframe fields support to `SchemaDatasetFieldsFacet`. Also include field comment as `description`.*
* **Spark: add `SparkApplicationDetailsFacet`** [`#2688`](https://github.com/OpenLineage/OpenLineage/pull/2688) [@dolfinus](https://github.com/dolfinus)  
    *Adds `SparkApplicationDetailsFacet` to `runEvent`s emitted on Spark application start.*

### Removed
* **Airflow: remove Airflow < 2.3.0 support** [`#2710`](https://github.com/OpenLineage/OpenLineage/pull/2710) [@kacpermuda](https://github.com/kacpermuda)  
    *Removes Airflow < 2.3.0 support.*
* **Integration: use v2 Python facets** [`#2693`](https://github.com/OpenLineage/OpenLineage/pull/2693) [@JDarDagran](https://github.com/JDarDagran)  
    *Migrates integrations from removed v1 facets to v2 Python facets.*

### Fixed
* **Spark: improve job suffix assigning mechanism** [`#2665`](https://github.com/OpenLineage/OpenLineage/pull/2665) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *For some catalog handlers, the mechanism was creating different dataset identifiers on START and COMPLETE depending on whether a dataset was created or not. This improves the mechanism to assign a deterministic job suffix based on the output dataset at the moment of a start event. **Note**: this may change job names in some scenarios.*
* **Airflow: fix empty dataset name for `AthenaExtractor`** [`#2700`](https://github.com/OpenLineage/OpenLineage/pull/2700) [@kacpermuda](https://github.com/kacpermuda)  
    *The dataset name should not be empty when passing only a bucket as S3 output in Athena.*
* **Flink: fix `SchemaDatasetFacet` for Protobuf repeated primitive types** [`#2685`](https://github.com/OpenLineage/OpenLineage/pull/2685) [@dolfinus](https://github.com/dolfinus)  
    *Fixes issues with the Protobuf schema converter.*
* **Python: clean up Python client code, add logging.** [`#2653`](https://github.com/OpenLineage/OpenLineage/pull/2653) [@kacpermuda](https://github.com/kacpermuda)  
    *Cleans up client code, refactors logging in all Python modules.*
* **SQL: catch `TokenizerError`s, `PanicException`** [`#2703`](https://github.com/OpenLineage/OpenLineage/pull/2703) [@mobuchowski](https://github.com/mobuchowski)  
    *The SQL parser now catches and handles these errors.*
* **Python: suppress warning on importing v1 module in __init__.py.** [`#2713`](https://github.com/OpenLineage/OpenLineage/pull/2713) [@JDarDagran](https://github.com/JDarDagran)  
    *Suppresses the deprecation warning when v1 facets are used.*
* **Integration/Java/Python: use UUIDv7 instead of UUIDv4** [`#2686`](https://github.com/OpenLineage/OpenLineage/pull/2686) [`#2687`](https://github.com/OpenLineage/OpenLineage/pull/2687) [@dolfinus](https://github.com/dolfinus)  
    *Uses UUIDv7 instead of UUIDv4 for `runEvent`s. The new UUID version produces monotonically increasing values, which leads to more performant queries on the OL consumer side. **Note**: UUID version is an implementation detail and can be changed in the future.*

## [1.14.0](https://github.com/OpenLineage/OpenLineage/compare/1.13.1...1.14.0) - 2024-05-09
### Added
* **Common/dbt: add DREMIO to supported dbt profile types** [`#2674`](https://github.com/OpenLineage/OpenLineage/pull/2674) [@surisimran](https://github.com/surisimran)  
    *Adds support for dbt-dremio, resolving [`#2668`](https://github.com/OpenLineage/OpenLineage/issues/2668).
* **Flink: support Protobuf format for sources and sinks** [`#2482`](https://github.com/OpenLineage/OpenLineage/pull/2482) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds schema extraction from Protobuf classes. Includes support for nested object types, `array` type, `map` type, `oneOf` and `any`.*
* **Java: add facet conversion test** [`#2663`](https://github.com/OpenLineage/OpenLineage/pull/2663) [@julienledem](https://github.com/julienledem)  
    *Adds a simple test that shows how to deserialize a facet in the server model.*
* **Spark: job type facet to distinguish RDD jobs from Spark SQL jobs** [`#2652`](https://github.com/OpenLineage/OpenLineage/pull/2652) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)    
    *Sets the `jobType` property of `JobTypeJobFacet` to either `SQL_JOB` or `RDD_JOB`.*
* **Spark: add Glue symlink if reading from Glue catalog table** [`#2646`](https://github.com/OpenLineage/OpenLineage/pull/2646) [@mobuchowski](https://github.com/mobuchowski)  
    *The dataset symlink now points to the Glue catalog table name if the Glue catalog table is used.*
* **Spark: add `spark_jobDetails` facet** [`#2662`](https://github.com/OpenLineage/OpenLineage/pull/2662) [@dolfinus](https://github.com/dolfinus)  
    *Adds a `SparkJobDetailsFacet`, capturing information about Spark application jobs -- e.g. `jobId`, `jobDescription`, `jobGroup`, `jobCallSite`. This allows for tracking an OpenLineage `RunEvent` with a specific Spark job in SparkUI.*

### Removed
* **Airflow: drop old `ParentRunFacet` key** [`#2660`](https://github.com/OpenLineage/OpenLineage/pull/2660) [@dolfinus](https://github.com/dolfinus)  
    *Changes the integration to use the `parent` key for `ParentFacet`, dropping the outdated `parentRun`.*
* **Spark: drop `SparkVersionFacet`** [`#2659`](https://github.com/OpenLineage/OpenLineage/pull/2659) [@dolfinus](https://github.com/dolfinus)  
    *Drops the `SparkVersion` facet, deprecated since 1.2.0 and planned for removal since 1.4.0.*
* **Python: allow relative paths in URI formats for Python facets** [`#2679`](https://github.com/OpenLineage/OpenLineage/pull/2679) [@JDarDagran](https://github.com/JDarDagran)  
    *Removes a URI validator that checked if scheme and netloc were present, allowing relative paths in URI formats for Python facets.*

### Changed
* **GreatExpectations: rename `ParentRunFacet` key** [`#2661`](https://github.com/OpenLineage/OpenLineage/pull/2661) [@dolfinus](https://github.com/dolfinus)  
    *The OpenLineage spec defined the `ParentRunFacet` with the property name parent but the Great Expectations integration created a lineage event with `parentRun`. This renames `ParentRunFacet` key from `parentRun` to `parent`. For backwards compatibility, keep the old name.*

### Fixed
* **dbt: support a less ambiguous logic to generate job names** [`#2658`](https://github.com/OpenLineage/OpenLineage/pull/2658) [@blacklight](https://github.com/blacklight)  
    *Includes profile and models in the dbt job name to make it more unique.*
* **Spark: update to use `org.apache.commons.lang3` instead of `org.apache.commons.lang`** [`#2676`](https://github.com/OpenLineage/OpenLineage/pull/2676) [@harels](https://github.com/harels)  
    *Updates Apache Commons Lang to the latest version. We were mixing two versions, and the old one was not present in many places.*

## [1.13.1](https://github.com/OpenLineage/OpenLineage/compare/1.12.0...1.13.1) - 2024-04-25
### Added
* **Java: allow timeout for circuit breakers** [`#2609`](https://github.com/OpenLineage/OpenLineage/pull/2609) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)[@athityakumar](https://github.com/athityakumar)  
    *Extends the circuit breaker mechanism to contain a global timeout that stops running OpenLineage integration code when a specified amount of time has elapsed.*
* **Java: handle `DataSetEvent` and `JobEvent` in `Transport.emit`** [`#2611`](https://github.com/OpenLineage/OpenLineage/pull/2611) [@dolfinus](https://github.com/dolfinus)  
    *Adds overloads `Transport.emit(OpenLineage.DatasetEvent)` and `Transport.emit(OpenLineage.JobEvent)`, reusing the implementation of `Transport.emit(OpenLineage.RunEvent)`. **Please note**: `Transport.emit(String)` is now deprecated and will be removed in 1.16.0.*
* **Java/Python: add `GZIP` compression to `HttpTransport`** [`#2603`](https://github.com/OpenLineage/OpenLineage/pull/2603) [`#2604`](https://github.com/OpenLineage/OpenLineage/pull/2604) [@dolfinus](https://github.com/dolfinus)  
    *Adds a `compression` option to `HttpTransport` config in the Java and Python clients, with `gzip` implementation.*
* **Java/Python/Proxy: properly set Kafka message key** [`#2571`](https://github.com/OpenLineage/OpenLineage/pull/2571) [`#2597`](https://github.com/OpenLineage/OpenLineage/pull/2597) [`#2598`](https://github.com/OpenLineage/OpenLineage/pull/2598) [@dolfinus](https://github.com/dolfinus)  
    *Adds a new `messageKey` option to `KafkaTransport` config in the Python and Java clients, as well as the Proxy. This option replaces the `localServerId` option, which is now deprecated. Default value is generated using the run id (for `RunEvent`), job name (for `JobEvent`) or dataset name (for `DatasetEvent`). This value is used by the Kafka producer to distribute messages along topic partitions, instead of sending all the events to the same partition. This allows for full utilization of Kafka performance advantages.*
* **Flink: add support for Micrometer metrics** [`#2633`](https://github.com/OpenLineage/OpenLineage/pull/2633) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds a mechanism for forwarding metrics to any [Micrometer-compatible implementation](https://docs.micrometer.io/micrometer/reference/implementations.html) for Flink as has been implemented for Spark. Included: `MeterRegistry`, `CompositeMeterRegistry`, `SimpleMeterRegistry`, and `MicrometerProvider`.*
* **Python: generate Python facets from JSON schemas** [`#2520`](https://github.com/OpenLineage/OpenLineage/pull/2520) [@JDarDagran](https://github.com/JDarDagran)  
    *Objects specified with JSON Schema needed to be manually developed and checked in Python, leading to many discrepancies, including wrong schema URLs. This adds a `datamodel-code-generator` for parsing JSON Schema and generating Pydantic or dataclasses classes, etc. In order to use `attrs` (a more modern version of dataclasses) and overcome some limitations of the tool, a number of steps have been added in order to customize code to meet OpenLineage requirements. Included: updated references to the latest base JSON Schema spec for all child facets. **Please note**: newly generated code creates a v2 interface that will be implemented in existing integrations in a future release. The v2 interface introduces some breaking changes: facets are put into separate modules per JSON Schema spec file, some names are changed, and several classes are now `kw_only`.*
* **Spark/Flink/Java: support YAML config files together with SparkConf/FlinkConf** [`#2583`](https://github.com/OpenLineage/OpenLineage/pull/2583) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Creates a `SparkOpenlineageConfig` and `FlinkOpenlineageConfig` for a more uniform configuration experience for the user. Renames `OpenLineageYaml` to `OpenLineageConfig` and modifies the code to use only `OpenLineageConfig` classes. Includes a doc update to mention that both ways can be used interchangeably and final documentation will merge all values provided.*
* **Spark: add custom token provider support** [`#2613`](https://github.com/OpenLineage/OpenLineage/pull/2613) [@tnazarew](https://github.com/tnazarew)  
    *Adds a `TokenProviderTypeIdResolver` to handle both `FQCN` and (for backward compatibility) `api_key` types in `spark.openlineage.transport.auth.type`.*
* **Spark/Flink: job ownership facet** [`#2533`](https://github.com/OpenLineage/OpenLineage/pull/2533) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)    
    *Enables configuration entries specifying ownership of the job that will result in an `OwnershipJobFacet` being attached to job facets.*

### Changed
* **Java: sync Kinesis `partitionKey` format with Kafka implementation** [`#2620`](https://github.com/OpenLineage/OpenLineage/pull/2620) [@dolfinus](https://github.com/dolfinus)  
    *Changes the format of Kinesis `partitionKey` from `{jobNamespace}:{jobName}` to `run:{jobNamespace}/{jobName}` to match the Kafka transport implementation.*
    
### Fixed
* **Python: make `load_config` return an empty dict instead of `None` when file empty** [`#2596`](https://github.com/OpenLineage/OpenLineage/pull/2596) [@kacpermuda](https://github.com/kacpermuda)  
    *`utils.load_config()` now returns an empty dict instead of `None` in the case of an empty file to prevent an `OpenLineageClient` crash.*
* **Java: render lombok-generated methods in javadoc** [`#2614`](https://github.com/OpenLineage/OpenLineage/pull/2614) [@dolfinus](https://github.com/dolfinus)  
    *Fixes rendering of javadoc for methods generated by `lombok` annotations by adding a `delombok` step.*
* **Spark/Snowflake: parse NPE when query option is used and table is empty** [`#2599`](https://github.com/OpenLineage/OpenLineage/pull/2599) [@mobuchowski](https://github.com/mobuchowski)  
    *Fixes NPE when using query option when reading from Snowflake.*

## [1.12.0](https://github.com/OpenLineage/OpenLineage/compare/1.11.3...1.12.0) - 2024-04-09

### Added
* **Airflow: add `lineage_job_namespace` and `lineage_job_name` macros** [`#2582`](https://github.com/OpenLineage/OpenLineage/pull/2582) [@dolfinus](https://github.com/dolfinus)
    *Adds new Airflow macros `lineage_job_namespace()`, `lineage_job_name(task)` that return an Airflow namespace and Airflow job name, respectively.*
* **Spec: allow nested struct fields in `SchemaDatasetFacet`** [`#2548`](https://github.com/OpenLineage/OpenLineage/pull/2548) [@dolfinus](https://github.com/dolfinus)
    *Allows nested fields support to `SchemaDatasetFacet`.*

### Fixed
* **Airflow: fix format returned by `airflow.macros.lineage_parent_id`** [`#2578`](https://github.com/OpenLineage/OpenLineage/pull/2578) [@blacklight](https://github.com/blacklight)  
    *Fixes the run format returned by the `lineage_parent_id` Airflow macro and simplifies the format of the `lineage_parent_id` and `lineage_run_id` macros.*
* **Dbt: propagate the dbt return code also when no OpenLineage events are emitted** [`#2591`](https://github.com/OpenLineage/OpenLineage/pull/2591) [@blacklight](https://github.com/blacklight)  
    *`dbt-ol` now propagates the exit code of the underlying dbt process even if no lineage events are emitted.*
* **Dagster: limit Dagster version to 1.6.9** [`#2579`](https://github.com/OpenLineage/OpenLineage/pull/2579) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds an upper limit on supported versions of Dagster as the integration is no longer actively maintained and recent releases introduce breaking changes.*
* **Java: make sure string isn't empty to prevent going out of bounds** [`#2585`](https://github.com/OpenLineage/OpenLineage/pull/2585) [@harels](https://github.com/harels)  
    *String lookup was not accounting for empty strings and causing a `java.lang.StringIndexOutOfBoundsException`.*
* **Java: fix javadoc** [`#2624`](https://github.com/OpenLineage/OpenLineage/pull/2624) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Improves developer experience by fixing issues resulting in warnings on build.*
* **Python: fix missing `pkg_resources` module on Python 3.12** [`#2572`](https://github.com/OpenLineage/OpenLineage/pull/2572) [@dolfinus](https://github.com/dolfinus)  
    *Removes `pkg_resources` dependency and replaces it with the [packaging](https://packaging.pypa.io/en/latest/version.html) lib.*
* **Spark: use `HashSet` in column-level lineage instead of iterating through `LinkedList`** [`#2584`](https://github.com/OpenLineage/OpenLineage/pull/2584) [@mobuchowski](https://github.com/mobuchowski)  
    *Takes advantage of performance gains available from using `HashSet` for collection.*

## [1.11.3](https://github.com/OpenLineage/OpenLineage/compare/1.10.2...1.11.3) - 2024-04-04

### Added
* **Common: add support for `SCRIPT`-type jobs in BigQuery** [`#2564`](https://github.com/OpenLineage/OpenLineage/pull/2564) [@kacpermuda](https://github.com/kacpermuda)  
    In the case of `SCRIPT`-type jobs in BigQuery, no lineage was being extracted because the `SCRIPT` job had no lineage information - it only spawned child jobs that had that information. With this change, the integration extracts lineage information from child jobs when dealing with `SCRIPT`-type jobs.
* **Spark: support for built-in lineage extraction** [`#2272`](https://github.com/OpenLineage/OpenLineage/pull/2272) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *This PR adds a `spark-interfaces-scala` package that allows lineage extraction to be implemented within Spark extensions (Iceberg, Delta, GCS, etc.). The Openlineage integration, when traversing the query plan, verifies if nodes implement defined interfaces. If so, interface methods are used to extract lineage. Refer to the [README](https://github.com/OpenLineage/OpenLineage/tree/spark/built-in-lineage/integration/spark-interfaces-scala#readme) for more details.*
* **Spark/Java: add support for Micrometer metrics** [`#2496`](https://github.com/OpenLineage/OpenLineage/pull/2496) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds a mechanism for forwarding metrics to any [Micrometer-compatible implementation](https://docs.micrometer.io/micrometer/reference/implementations.html). Included: `MeterRegistryFactory`, `MicrometerProvider`, `StatsDMetricsBuilder`, metrics config in OpenLineage config, and a Java client implementation.*
* **Spark: add support for telemetry mechanism** [`#2528`](https://github.com/OpenLineage/OpenLineage/pull/2528) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds timers, counters and additional instrumentation in order to implement Micrometer metrics collection.*
* **Spark: support query option on table read** [`#2556`](https://github.com/OpenLineage/OpenLineage/pull/2556) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds support for the Spark-BigQuery connector's query input option, which executes a query directly on BigQuery, storing the result in an intermediate dataset, bypassing Spark's computation layer. Due to this, the lineage is retrieved using the SQL parser, similarly to `JDBCRelation`.*
* **Spark: change `SparkPropertyFacetBuilder` to support recording Spark runtime** [`#2523`](https://github.com/OpenLineage/OpenLineage/pull/2523) [@Ruihua98](https://github.com/Ruihua98)  
    *Modifies `SparkPropertyFacetBuilder` to capture the `RuntimeConfig` of the Spark session because the existing `SparkPropertyFacet` can only capture the static config of the Spark context. This facet will be added in both RDD-related and SQL-related runs.*
* **Spec: add `fileCount` to dataset stat facets** [`#2562`](https://github.com/OpenLineage/OpenLineage/pull/2562) [@dolfinus](https://github.com/dolfinus)  
    *Adds a `fileCount` field to `DataQualityMetricsInputDatasetFacet` and `OutputStatisticsOutputDatasetFacet` specification.*

### Fixed
* **dbt: `dbt-ol` should transparently exit with the same exit code as the child `dbt` process** [`#2560`](https://github.com/OpenLineage/OpenLineage/pull/2560) [@blacklight](https://github.com/blacklight)  
    *Makes `dbt-ol` transparently exit with the same exit code as the child `dbt` process.*
* **Flink: disable module metadata generation** [`#2531`](https://github.com/OpenLineage/OpenLineage/pull/2531) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Disables the module metadata generation for Flink to fix the problem of having gradle dependencies to submodules within `openlineage-flink.jar`.*
* **Flink: fixes to version 1.19** [`#2507`](https://github.com/OpenLineage/OpenLineage/pull/2507) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes the class not found issue when checking for Cassandra classes. Also fixes the Maven pom dependency on subprojects.*
* **Python: small improvements to `.emit()` method logging & annotations** [`#2539`](https://github.com/OpenLineage/OpenLineage/pull/2539) [@dolfinus](https://github.com/dolfinus)  
    *Updates OpenLineage.emit debug messages and annotations.*
* **SQL: show error message when OpenLineageSql cannot find native library** [`#2547`](https://github.com/OpenLineage/OpenLineage/pull/2547) [@dolfinus](https://github.com/dolfinus)  
    *When the `OpenLineageSql` class could not load a native library, if returned `None` for all operations. But because the error message was suppressed, the user could not determine the reason.*
* **SQL: update code to conform to upstream sqlparser-rs changes** [`#2510`](https://github.com/OpenLineage/OpenLineage/pull/2510) [@mobuchowski](https://github.com/mobuchowski)  
    *Includes tests and cosmetic improvements.*
* **Spark: fix access to active Spark session** [`#2535`](https://github.com/OpenLineage/OpenLineage/pull/2535) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Changes behavior so `IllegalStateException` is always caught when accessing `SparkSession`.*
* **Spark: fix Databricks environment** [`#2537`](https://github.com/OpenLineage/OpenLineage/pull/2537) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes the `ClassNotFoundError` occurring on Databricks runtime and extends the integration test to verify `DatabricksEnvironmentFacet`.*
* **Spark: fixed memory leak in JobMetricsHolder** [`#2565`](https://github.com/OpenLineage/OpenLineage/pull/2565) [@d-m-h](https://github.com/d-m-h)
    *The `JobMetricsHolder#cleanUp(int)` method now correctly purges unneeded state from both maps.*
* **Spark: fixed memory leak in `UnknownEntryFacetListener`** [`#2557`](https://github.com/OpenLineage/OpenLineage/pull/2557) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
    *Prevents storing the state when a facet is disabled, purging the state after populating run facets.*
* **Spark: fix parsing `JDBCOptions(table=...)` containing subquery** [`#2546`](https://github.com/OpenLineage/OpenLineage/pull/2546) [@dolfinus](https://github.com/dolfinus)  
    *Prevents `openlineage-spark` from producing datasets with names like `database.(select * from table)` for JDBC sources.*
* **Spark/Snowflake: support query option via SQL parser** [`#2563`](https://github.com/OpenLineage/OpenLineage/pull/2563) [@mobuchowski](https://github.com/mobuchowski)  
    *When a Snowflake job is bypassing Spark's computation layer, now the SQL parser will be used to get the lineage.*
* **Spark: always catch `IllegalStateException` when accessing `SparkSession`** [`#2535`](https://github.com/OpenLineage/OpenLineage/pull/2535) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *`IllegalStateException` was not being caught.*

## [1.10.2](https://github.com/OpenLineage/OpenLineage/compare/1.9.1...1.10.2) - 2024-03-15

### Added
* **Dagster: add new provider for version 1.6.10** [`#2518`](https://github.com/OpenLineage/OpenLineage/pull/2518) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds the new provider required by the latest version of Dagster.*
* **Flink: support lineage for a hybrid source** [`#2491`](https://github.com/OpenLineage/OpenLineage/pull/2491) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Adds support for hybrid source lineage for users of Kafka and Iceberg sources in backfill usecases.*
* **Flink: improve Cassandra lineage metadata** [`#2479`](https://github.com/OpenLineage/OpenLineage/pull/2479) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Cassandra cluster info to be used as the dataset namespace, and the keyspace to be combined with the table name as the dataset name.*
* **Flink: bump Flink JDBC connector version** [`#2472`](https://github.com/OpenLineage/OpenLineage/pull/2472) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Bumps the Flink JDBC connector version to 3.1.2-1.18 for Flink 1.18.*
* **Java: add a `OpenLineageClientUtils#loadOpenLineageJson(InputStream)` and change `OpenLineageClientUtils#loadOpenLineageYaml(InputStream)` methods** [`#2490`](https://github.com/OpenLineage/OpenLineage/pull/2490) [@d-m-h](https://github.com/d-m-h)  
    *This improves the explicitness of the methods. Previously, `loadOpenLineageYaml(InputStream)` wanted the `InputStream` to contain bytes that represented JSON.*
* **Java: add info from the HTTP response to the client exception** [`#2486`](https://github.com/OpenLineage/OpenLineage/pull/2486) [@davidjgoss](https://github.com/davidjgoss)  
    *Adds the status code and body as properties on the thrown exception when a non-success response is encountered in the HTTP transport.*
* **Python: add support for MSK IAM authentication with a new transport** [`#2478`](https://github.com/OpenLineage/OpenLineage/pull/2478) [@mattiabertorello](https://github.com/mattiabertorello)  
    *Eases publication of events to MSK with IAM authentication.*

### Removed
* **Airflow: remove redundant information from facets** [`#2524`](https://github.com/OpenLineage/OpenLineage/pull/2524) [@kacpermuda](https://github.com/kacpermuda)  
    *Refines the operator's attribute inclusion logic in facets to include only those known to be important or compact, ensuring that custom operator attributes with substantial data do not inflate the event size.*

### Fixed
* **Airflow: proceed without rendering templates if `task_instance` copy fails** [`#2492`](https://github.com/OpenLineage/OpenLineage/pull/2492) [@kacpermuda](https://github.com/kacpermuda)  
    *Airflow will now proceed without rendering templates if `task_instance` copy fails in `listener.on_task_instance_running`.*
* **Spark: fix the `HttpTransport` timeout** [`#2475`](https://github.com/OpenLineage/OpenLineage/pull/2475) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *The existing `timeout` config parameter is ambiguous: implementation treats the value as double in seconds, although the documentation claims it's milliseconds. A new config param `timeoutInMillis` has been added. the Existing `timeout` has been removed from docs and will be deprecated in 1.13.*
* **Spark: prevent NPE if the context is null** [`#2515`](https://github.com/OpenLineage/OpenLineage/pull/2515) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a check for a null context before executing `end(jobEnd)`.*
* **Flink: fix class not found issue for Cassandra** [`#2507`](https://github.com/OpenLineage/OpenLineage/pull/2507) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
   *Fixes the class not found issue when checking for Cassandra classes. Also fixes the Maven POM dependency on subprojects.*
* **Flink: refine the JDBC table name** [`#2512`](https://github.com/OpenLineage/OpenLineage/pull/2512) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Enables the JDBC table name with a schema prefix.*
* **Flink: fix JDBC dataset naming** [`#2508`](https://github.com/OpenLineage/OpenLineage/pull/2508) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *For JDBC, the Flink integration is not adjusted to the Openlineage naming convention. There is code that extracts the dataset namespace/name from the JDBC connection url, but it's in the Spark integration. As a solution, this code has to be extracted into the Java client and reused by the Spark and Flink integrations.*
* **Flink: fix failure due to missing Cassandra classes** [`#2507`](https://github.com/OpenLineage/OpenLineage/pull/2507) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Flink is failing when no Cassandra classes are present on the class path. This is happening because of `CassandraUtils` class which has a static `hasClasses` method, but it imports Cassandra-related classes in the header. Also, the Flink subproject contains an unnecessary `maven-publish` plugin.*
* **Flink: fix release runtime dependencies** [`#2504`](https://github.com/OpenLineage/OpenLineage/pull/2504) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *The shadow jar of Flink is not minimized, so some internal jars are listed as runtime dependences. This removes them from the final pom.xml file in the Flink module.*
* **Spec: improve Cassandra lineage metadata** [`#2479`](https://github.com/OpenLineage/OpenLineage/pull/2479) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Following the namespace definition, we should use `cassandra://host:port`.*

## [1.9.1](https://github.com/OpenLineage/OpenLineage/compare/1.8.0...1.9.1) - 2024-02-26
### Added
* **Airflow: add support for `JobTypeJobFacet` properties** [`#2412`](https://github.com/OpenLineage/OpenLineage/pull/2412) [@mattiabertorello](https://github.com/mattiabertorello)  
    *Adds support for Job type properties within the Airflow Job facet.*
* **dbt: add support for `JobTypeJobFacet` properties** [`#2411`](https://github.com/OpenLineage/OpenLineage/pull/2411) [@mattiabertorello](https://github.com/mattiabertorello)  
    *Support Job type properties within the DBT Job facet.*
* **Flink: support Flink Kafka dynamic source and sink** (https://github.com/OpenLineage/OpenLineage/pull/2417) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Adds support for Flink Kafka Table Connector use cases for topic and schema extraction.*
* **Flink: support multi-topic Kafka Sink** [`#2372`](https://github.com/OpenLineage/OpenLineage/pull/2372) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for multi-topic Kafka sinks. Limitations: `recordSerializer` needs to implement `KafkaTopicsDescriptor`. Please refer to the [limitations](https://openlineage.io/docs/integrations/flink/#limitations) sections in documentation.*
* **Flink: support lineage for JDBC connector** [`#2436`](https://github.com/OpenLineage/OpenLineage/pull/2436) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Adds support for use cases that employ this connector.*
* **Flink: add common config gradle plugin** (https://github.com/OpenLineage/OpenLineage/pull/2461) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Add common config gradle plugin to simplify gradle files of Flink submodules.*
* **Java: extend circuit breaker loaded with `ServiceLoader`** [`#2435`](https://github.com/OpenLineage/OpenLineage/pull/2435) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Loads the circuit breaker builder with `ServiceLoader` as an addition to a list of implemented builders available within the existing package.*
* **Spark: integration now emits intermediate, application level events wrapping entire job execution** [`#2371`](https://github.com/OpenLineage/OpenLineage/pull/2471) [@mobuchowski](https://github.com/mobuchowski)  
    *Previously, the Spark event model described only single actions, potentially linked only to some parent run. Closes [`#1672`](https://github.com/OpenLineage/OpenLineage/issues/1672).*
* **Spark: support built-in lineage within `DataSourceV2Relation`** [`#2394`](https://github.com/OpenLineage/OpenLineage/pull/2394) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Enables built-in lineage extraction within from `DataSourceV2Relation` lineage nodes.*
* **Spark: add support for `JobTypeJobFacet` properties** [`#2410`](https://github.com/OpenLineage/OpenLineage/pull/2410) [@mattiabertorello](https://github.com/mattiabertorello)  
    *Adds support for Job type properties within the Spark Job facet.*
* **Spark: stop sending  `spark.LogicalPlan` facet by default** [`#2433`](https://github.com/OpenLineage/OpenLineage/pull/2433) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *`spark.LogicalPlan` has been added to default value of `spark.openlineage.facets.disabled`.*
* **Spark/Flink/Java: circuit breaker** [`#2407`](https://github.com/OpenLineage/OpenLineage/issues/2407) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Introduces a circuit breaker mechanism to prevent effects of over-instrumentation. Implemented within Java client, it serves both the Flink and Spark integration. Read the Java client README for more details.*
* **Spark: add the capability to publish Scala 2.12 and 2.13 variants of `openlineage-spark`** [`#2446`](https://github.com/OpenLineage/OpenLineage/pull/2446) [@d-m-h](https://github.com/d-m-h)  
    *Adds the capability to publish Scala 2.12 and 2.13 variants of `openlineage-spark`* 

### Changed
* **Spark: enable the `app` module to be compiled with Scala 2.12 and Scala 2.13 variants of Apache Spark** (https://github.com/OpenLineage/OpenLineage/pull/2432) [@d-m-h](https://github.com/d-m-h)  
    *The `spark.binary.version` and `spark.version` properties control which variant to build.*
* **Spark: enable Scala 2.13 support in the `app` module** [`#2432`](https://github.com/OpenLineage/OpenLineage/pull/2432) [@d-m-h](https://github.com/d-m-h)  
    *Enables the `app` module to be built using both Scala 2.12 and Scala 2.13 variants of various Apache Spark versions, and enables the CI/CD pipeline to build and test them.*
* **Spark: don't fail on exception of `UnknownEntryFacet` creation** [`#2431`](https://github.com/OpenLineage/OpenLineage/pull/2431) [@mobuchowski](https://github.com/mobuchowski)  
    *Failure to generate `UnknownEntryFacet` was resulting in the event not being sent.*
* **Spark: move Snowflake code into the vendor projects folders** [`#2405`](https://github.com/OpenLineage/OpenLineage/pull/2405) [@mattiabertorello](https://github.com/mattiabertorello)  
    *Creates a `vendor` folder to isolate Snowflake-specific code from the main Spark integration, enhancing organization and flexibility.*

### Fixed
* **Flink: resolve PMD rule violation warnings** [`#2403`](https://github.com/OpenLineage/OpenLineage/pull/2403) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Resolves the PMD rule violation warnings in the Flink integration module.*
* **Flink: Added the 'isReleaseVersion' property back to the build, enabling the Flink integration to be release** [`#2468`](https://github.com/OpenLineage/OpenLineage/pull/2468) [@d-m-h](https://github.com/d-m-h)  
    *The 'isReleaseVersion' property was removed from the build, preventing the Flink integration from being released.*
* **Python: fix issue with file config creating additional file** [`#2447`](https://github.com/OpenLineage/OpenLineage/pull/2447) [@kacpermuda](https://github.com/kacpermuda)  
    *`FileConfig` was creating an additional file when not in append mode. Closes [`#2439`](https://github.com/OpenLineage/OpenLineage/issues/2439).*
* **Python: fix issue with append option in file config** [`#2441`](https://github.com/OpenLineage/OpenLineage/pull/2441) [@kacpermuda](https://github.com/kacpermuda)  
    *`FileConfig` was ignoring the append key in YAML config. Closes [`#2440`](https://github.com/OpenLineage/OpenLineage/issues/2440)*
* **Spark: fix integration catalog symlink without warehouse** [`#2379`](https://github.com/OpenLineage/OpenLineage/pull/2379) [@algorithmy1](https://github.com/algorithmy1)  
    *In the case of symlinked Glue Catalog Tables, the parsing method was producing dataset names identical to the namespace.*
* **Flink: fix `IcebergSourceWrapper` for Iceberg connector 1.17** [`#2409`](https://github.com/OpenLineage/OpenLineage/pull/2409) [@ensctom](https://github.com/ensctom)  
    *In Flink 1.17, the Iceberg `catalogloader` was loading the catalog in the open function, causing the `loadTable` method to throw a `NullPointerException` error.*
* **Spark: migrate `spark35`, `spark3`, `shared` modules to produce Scala 2.12 and Scala 2.13 variants** [`#2390`](https://github.com/OpenLineage/OpenLineage/pull/2390) [`#2385`](https://github.com/OpenLineage/OpenLineage/pull/2385)[`#2384`](https://github.com/OpenLineage/OpenLineage/pull/2384) [@d-m-h](https://github.com/d-m-h)  
    *Migrates the three modules to use the refactored Gradle plugins. Also splits some tests into Scala 2.12- and Scala 2.13-specific versions.*
* **Spark: conform the `spark2` module to the new build process** [`#2391`](https://github.com/OpenLineage/OpenLineage/pull/2391) [@d-m-h](https://github.com/d-m-h)  
    *Due to a change in the Scala Collections API in Scala 2.13, `NoSuchMethodErrors` were being thrown when running the openlineage-spack connector in an Apache Spark runtime compiled using Scala 2.13.*

## [1.8.0](https://github.com/OpenLineage/OpenLineage/compare/1.7.0...1.8.0) - 2024-01-19

* **Flink: support Flink 1.18** [`#2366`](https://github.com/OpenLineage/OpenLineage/pull/2366) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Adds support for the latest Flink version with 1.17 used for Iceberg Flink runtime and Cassandra Connector as these do not yet support 1.18.*
* **Spark: add Gradle plugins to simplify the build process to support Scala 2.13** [`#2376`](https://github.com/OpenLineage/OpenLineage/pull/2376) [@d-m-h](https://github.com/d-m-h)  
    *Defines a set of Gradle plugins to configure the modules and reduce duplication.
* **Spark: support multiple Scala versions `LogicalPlan` implementation** [`#2361`](https://github.com/OpenLineage/OpenLineage/pull/2361) [@mattiabertorello](https://github.com/mattiabertorello)  
    *In the LogicalPlanSerializerTest class, the implementation of the LogicalPlan interface is different between Scala 2.12 and Scala 2.13. In detail, the IndexedSeq changes package from the scala.collection to scala.collection.immutable. This implements both of the methods necessary in the two versions.*
* **Spark: Use ScalaConversionUtils to convert Scala and Java collections** [`#2357`](https://github.com/OpenLineage/OpenLineage/pull/2357) [@mattiabertorello](https://github.com/mattiabertorello)  
    *This initial step is to start supporting compilation for Scala 2.13 in the 3.2+ Spark versions. Scala 2.13 changed the default collection to immutable, the methods to create an empty collection, and the conversion between Java and Scala. This causes the code to not compile between 2.12 and 2.13. This replaces the usage of direct Scala collection methods (like creating an empty object) and conversions utils with `ScalaConversionUtils` methods that will support cross-compilation.*
* **Spark: support `MERGE INTO` queries on Databricks** [`#2348`](https://github.com/OpenLineage/OpenLineage/pull/2348) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Supports custom plan nodes used when running `MERGE INTO` queries on Databricks runtime.*
* **Spark: Support Glue catalog in iceberg** [`#2283`](https://github.com/OpenLineage/OpenLineage/pull/2283) [@nataliezeller1](https://github.com/nataliezeller1)  
    *Adds support for the Glue catalog based on the 'catalog-impl' property (in this case we will not have a 'type' property).*

### Changed

* **Spark: Move Spark 3.1 code from the spark3 project** [`#2365`](https://github.com/OpenLineage/OpenLineage/pull/2365) [@mattiabertorello](https://github.com/mattiabertorello)  
    *Moves the Spark 3.1-related code to a specific project, spark31, so the spark3 project can be compiled with any Spark 3.x version.*

### Fixed
* **Airflow: add database information to SnowflakeExtractor** [`#2364`](https://github.com/OpenLineage/OpenLineage/pull/2364) [@kacpermuda](https://github.com/kacpermuda)  
    *Fixes missing database information in SnowflakeExtractor.*
* **Airflow: add dag_id to task_run_id to avoid duplicates** [`#2358`](https://github.com/OpenLineage/OpenLineage/pull/2358) [@kacpermuda](https://github.com/kacpermuda)  
    *The lack of dag_id in task_run_id can cause duplicates in run_id across different dags.*
* **Airflow: Add tests for column lineage facet and sql parser** [`#2373`](https://github.com/OpenLineage/OpenLineage/pull/2373) [@kacpermuda](https://github.com/kacpermuda)  
    *Improves naming (database.schema.table) in SQLExtractor's column lineage facet and adds some unit tests.*
* **Spark: fix removePathPattern behaviour** [`#2350`](https://github.com/OpenLineage/OpenLineage/pull/2350) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *The removepath pattern feature is not applied all the time. The method is called when constructing DatasetIdentifier through PathUtils which is not the case all the time. This moves removePattern to another place in the codebase that is always run.*
* **Spark: fix a type incompatibility in RddExecutionContext between Scala 2.12 and 2.13** [`#2360`](https://github.com/OpenLineage/OpenLineage/pull/2360) [@mattiabertorello](https://github.com/mattiabertorello)  
    *The function from the ResultStage.func() object change type in Spark between Scala 2.12 and 2.13 makes the compilation fail. This avoids getting the function with an explicit type; instead, it gets it every time it is needed from the ResultStage object. This PR is part of the effort to support Scala 2.13 in the Spark integration.*
* **Spark: Fix `removePathPattern` feature** [`#2350`](https://github.com/OpenLineage/OpenLineage/pull/2350) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
   *Refactors code to make sure that all datasets sent are processed through `removePathPattern` if configured to do so.*
* **Spark: Clean up the individual build.gradle files in preparation for Scala 2.13 support** [`#2377`](https://github.com/OpenLineage/OpenLineage/pull/2377) [@d-m-h](https://github.com/d-m-h)  
    *Cleans up the build.gradle files, consolidating the custom plugin and removing unused and unnecessary configuration.*
* **Spark: refactor the Gradle plugins to make it easier to define Scala variants per module** [`#2383`](https://github.com/OpenLineage/OpenLineage/pull/2383) [@d-m-h](https://github.com/d-m-h)  
    *The third of several PRs to support producing Scala 2.12 and Scala 2.13 variants of the OpenLineage Spark integration. This PR refactors the custom Gradle plugins in order to make supporting multiple variants per module easier. This is necessary because the shared module fails its tests when consuming the Scala 2.13 variants of Apache Spark.*

## [1.7.0](https://github.com/OpenLineage/OpenLineage/compare/1.6.2...1.7.0) - 2023-12-21
### Added
* **Airflow: add parent run facet to `COMPLETE` and `FAIL` events in Airflow integration** [`#2320`](https://github.com/OpenLineage/OpenLineage/pull/2320) [@kacpermuda](https://github.com/kacpermuda)  
    *Adds a parent run facet to all events in the Airflow integration.*

### Fixed
* **Airflow: repair up.sh for MacOS** [`#2316`](https://github.com/OpenLineage/OpenLineage/pull/2316) [`#2318`](https://github.com/OpenLineage/OpenLineage/pull/2318) [@kacpermuda](https://github.com/kacpermuda)  
    *Some scripts were not working well on MacOS. This adjusts them.*
* **Airflow: repair `run_id` for FAIL event in Airflow 2.6+** [`#2305`](https://github.com/OpenLineage/OpenLineage/pull/2305) [@kacpermuda](https://github.com/kacpermuda)  
    *The `Run_id` in a `FAIL` event was different than in the `START` event for Airflow 2.6+.*
* **Flink: open Iceberg `TableLoader` before loading a table** [`#2314`](https://github.com/OpenLineage/OpenLineage/pull/2314) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes a potential `NullPointerException` in 1.17 when dealing with Iceberg sinks.*
* **Flink: name Kafka datasets according to the naming convention** [`#2321`](https://github.com/OpenLineage/OpenLineage/pull/2321) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a `kafka://` prefix to Kafka topic datasets' namespaces.*
* **Flink: fix properties within `JobTypeJobFacet`** [`#2325`](https://github.com/OpenLineage/OpenLineage/pull/2325) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes properties assignment in the Flink visitor.*
* **Spark: fix `commons-logging` relocate in target jar** [`#2319`](https://github.com/OpenLineage/OpenLineage/pull/2319) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Avoids relocating a dependency that was getting excluded from the jar.*
* **Spec: fix inconsistency with Redshift authority format** [`#2315`](https://github.com/OpenLineage/OpenLineage/pull/2315) [@davidjgoss](https://github.com/davidjgoss)  
    *Amends the `Authority` format for consistency with other references in the same section.*

### Removed
* **Airflow: remove Airflow 2.8+ support** [`#2330`](https://github.com/OpenLineage/OpenLineage/pull/2330) [@kacpermuda](https://github.com/kacpermuda)  
    *To encourage use of the Provider, this removes the listener from the plugin if the Airflow version is `<2.3.0` or `>=2.8.0`.*

## [1.6.2](https://github.com/OpenLineage/OpenLineage/compare/1.5.0...1.6.2) - 2023-12-07
### Added
* **Dagster: support Dagster 1.5.x** [`#2220`](https://github.com/OpenLineage/OpenLineage/pull/2220) [@tsungchih](https://github.com/tsungchih)  
    *Gets event records for each target Dagster event type to support Dagster version 0.15.0+.*
* **Dbt: add a new command `dbt-ol send-events` to send metadata of the last run without running the job** [`#2285`](https://github.com/OpenLineage/OpenLineage/pull/2285) [@sophiely](https://github.com/sophiely)  
    *Adds a new command to send events to OpenLineage according to the latest metadata generated without running any dbt command.*
* **Flink: add option for Flink job listener to read from Flink conf** [`#2229`](https://github.com/OpenLineage/OpenLineage/pull/2229) [@ensctom](https://github.com/ensctom)  
    *Adds option for the Flink job listener to read jobnames and namespaces from Flink conf.*
* **Spark: get column-level lineage from JDBC dbtable option** [`#2284`](https://github.com/OpenLineage/OpenLineage/pull/2284) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds support for dbtable, enables lineage in the case of single input columns, and improves dataset naming.*
* **Spec: introduce `JobTypeJobFacet` to contain additional job related information**[`#2241`](https://github.com/OpenLineage/OpenLineage/pull/2241) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *New `JobTypeJobFacet` contains the processing type such as `BATCH|STREAMING`, integration via `SPARK|FLINK|...` and job type in `QUERY|COMMAND|DAG|...`.*
* **SQL: add quote information from sqlparser-rs** [`#2259`](https://github.com/OpenLineage/OpenLineage/pull/2259) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds quote information from sqlparser-rs.*

### Fixed
* **Spark: update Jackson dependency to resolve `CVE-2022-1471`** [`#2185`](https://github.com/OpenLineage/OpenLineage/pull/2185) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Updates Gradle for Spark and Flink to 8.1.1. Upgrade Jackson `2.15.3`.*
* **Flink: avoid relying on Guava which can be missing during production runtime** [`#2296`](https://github.com/OpenLineage/OpenLineage/pull/2296) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Removes usage of Guava ImmutableList.*
* **Spark: exclude `commons-logging` transitive dependency from published jar** [`#2297`](https://github.com/OpenLineage/OpenLineage/pull/2297) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Ensures `commons-logging` is not shipped as this can lead to a version mismatch on the user's side.*

## [1.5.0](https://github.com/OpenLineage/OpenLineage/compare/1.4.1...1.5.0) - 2023-11-01
### Added
* **Flink: add Flink lineage for Cassandra Connectors** [`#2175`](https://github.com/OpenLineage/OpenLineage/pull/2175) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Adds Flink Cassandra source and sink visitors and Flink Cassandra Integration test.*
* **Spark: support `rdd` and `toDF` operations available in Spark Scala API** [`#2188`](https://github.com/OpenLineage/OpenLineage/pull/2188) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)   
    *Includes the first Scala integration test, fixes `ExternalRddVisitor` and adds support for extracting inputs from `MapPartitionsRDD` and `ParallelCollectionRDD` plan nodes.*
* **Spark: support Databricks Runtime 13.3** [`#2185`](https://github.com/OpenLineage/OpenLineage/pull/2185) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Modifies the Spark integration to support the latest Databricks Runtime version.*

### Changed
* **Airflow: loosen attrs and requests versions** [`#2107`](https://github.com/OpenLineage/OpenLineage/pull/2107) [@JDarDagran](https://github.com/JDarDagran)  
    *Lowers the version requirements for attrs and requests and removes an unnecessary dependency.*
* **dbt: render yaml configs lazily** [`#2221`](https://github.com/OpenLineage/OpenLineage/pull/2221) [@JDarDagran](https://github.com/JDarDagran)  
    *Don't render each entry in yaml files at start.* 

### Fixed
* **Airflow/Athena: change dataset name to its location** [`#2167`](https://github.com/OpenLineage/OpenLineage/pull/2167) [@sophiely](https://github.com/sophiely)  
    *Replaces the dataset and namespace with the data's physical location for more complete lineage across integrations.*
* **Python client: skip redaction in column lineage facet** [`#2177`](https://github.com/OpenLineage/OpenLineage/pull/2177) [@JDarDagran](https://github.com/JDarDagran)  
    *Redacted fields in `ColumnLineageDatasetFacetFieldsAdditionalInputFields` are now skipped.*
* **Spark: unify dataset naming for RDD jobs and Spark SQL** [`#2181`](https://github.com/OpenLineage/OpenLineage/pull/2181) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Use the same mechanism for RDD jobs to extract dataset identifier as used for Spark SQL.*
* **Spark: ensure a single `START` and a single `COMPLETE` event are sent** [`#2103`](https://github.com/OpenLineage/OpenLineage/pull/2103) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)   
    *For Spark SQL at least four events are sent triggered by different SparkListener methods. Each of them is required and used to collect facets unavailable elsewhere. However, there should be only one `START` and `COMPLETE` events emitted. Other events should be sent as `RUNNING`. Please keep in mind that Spark integration remains stateless to limit the memory footprint, and it is the backend responsibility to merge several Openlineage events into a meaningful snapshot of metadata changes.*

## [1.4.1](https://github.com/OpenLineage/OpenLineage/compare/1.3.1...1.4.1) - 2023-10-09
### Added
* **Client: allow setting client's endpoint via environment variable** [`#2151`](https://github.com/OpenLineage/OpenLineage/pull/2151) [@mars-lan](https://github.com/mars-lan)  
    *Enables setting this endpoint via environment variable because creating the client manually in Airflow is not possible.*
* **Flink: expand Iceberg source types** [`#2149`](https://github.com/OpenLineage/OpenLineage/pull/2149) [@HuangZhenQiu](https://github.com/HuangZhenQiu)  
    *Adds support for `FlinkIcebergSource` and `FlinkIcebergTableSource` for Flink Iceberg lineage.*
* **Spark: add debug facet** [`#2147`](https://github.com/OpenLineage/OpenLineage/pull/2147) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *An extra run facet containing some system details (e.g., OS, Java, Scala version), classpath (e.g., package versions, jars included in the Spark job), SparkConf (like openlineage entries except auth, specified extensions, etc.) and LogicalPlan details (execution tree nodes' names) are added to events emitted. SparkConf setting `spark.openlineage.debugFacet=enabled` needs to be set to include the facet. By default, the debug facet is disabled.*
* **Spark: enable Nessie REST catalog** [`#2165`](https://github.com/OpenLineage/OpenLineage/pull/2165) [@julwin](https://github.com/julwin)  
    *Adds support for Nessie catalog in Spark.*

## [1.3.1](https://github.com/OpenLineage/OpenLineage/compare/1.2.2...1.3.1) - 2023-10-03
### Added
* **Airflow: add some basic stats to the Airflow integration** [`#1845`](https://github.com/OpenLineage/OpenLineage/pull/1845) [@harels](https://github.com/harels)  
    *Uses the statsd component that already exists in the Airflow codebase and wraps the section that emits to event with a timer, as well as emitting a counter for exceptions in sending the event.*
* **Airflow: add columns as schema facet for `airflow.lineage.Table` (if defined)** [`#2138`](https://github.com/OpenLineage/OpenLineage/pull/2138) [@erikalfthan](https://github.com/erikalfthan)  
    *Adds columns (if set) from `airflow.lineage.Table` inlets/outlets to the OpenLineage Dataset.*
* **dbt: add SQLSERVER to supported dbt profile types** [`#2136`](https://github.com/OpenLineage/OpenLineage/pull/2136) [@erikalfthan](https://github.com/erikalfthan)  
    *Adds support for dbt-sqlserver, solving [#2129](https://github.com/OpenLineage/OpenLineage/issues/2129).*
* **Spark: support for latest 3.5** [`2118`](https://github.com/OpenLineage/OpenLineage/pull/2118) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Integration tests are now run on Spark 3.5. Also upgrades 3.3 branch to 3.3.3. Please note that `delta` and `iceberg` are not supported for Spark `3.5` at this time.*
* **Flink: expand iceberge source types** [`#2149`](https://github.com/OpenLineage/OpenLineage/pull/2149) [@HuangZhenQiu](https://github.com/HuangZhenQiu)
    *Add Iceberg Source and Iceberg Table Source for Flink Lineage.*

### Fixed
* **Airflow: fix find-links path in tox** [`#2139`](https://github.com/OpenLineage/OpenLineage/pull/2139) [@JDarDagran](https://github.com/JDarDagran)  
    *Fixes a broken link.*
* **Airflow: add more graceful logging when no OpenLineage provider installed** [`#2141`](https://github.com/OpenLineage/OpenLineage/pull/2141) [@JDarDagran](https://github.com/JDarDagran)  
    *Recognizes a failed import of `airflow.providers.openlineage` and adds more graceful logging to fix a corner case.*
* **Spark: fix bug in PathUtils' prepareDatasetIdentifierFromDefaultTablePath(CatalogTable) to correctly preserve scheme from CatalogTable's location** [`#2142`](https://github.com/OpenLineage/OpenLineage/pull/2142) [@d-m-h](https://github.com/d-m-h)  
    *Previously, the prepareDatasetIdentifierFromDefaultTablePath method would override the scheme with the value of "file" when constructing a dataset identifier. It now uses the scheme of the CatalogTable's URI for this. Thank you [@pawel-big-lebowski](https://github.com/pawel-big-lebowski) for the quick triage and suggested fix.* 

## [1.2.2](https://github.com/OpenLineage/OpenLineage/compare/1.1.0...1.2.2) - 2023-09-19
### Added
* **Spark: publish the `ProcessingEngineRunFacet` as part of the normal operation of the `OpenLineageSparkEventListener`** [`#2089`](https://github.com/OpenLineage/OpenLineage/pull/2089) [@d-m-h](https://github.com/d-m-h)  
    *Publishes the spec-defined `ProcessEngineRunFacet` alongside the custom `SparkVersionFacet` (for now).*
    *The `SparkVersionFacet` is deprecated and will be removed in a future release.*
* **Spark: capture and emit `spark.databricks.clusterUsageTags.clusterAllTags` variable from databricks environment** [`#2099`](https://github.com/OpenLineage/OpenLineage/pull/2099) [@Anirudh181001](https://github.com/Anirudh181001)  
    *Adds `spark.databricks.clusterUsageTags.clusterAllTags` to the list of environment variables captured from databricks.*
     
### Fixed
* **Common: support parsing dbt_project.yml without target-path** [`#2106`](https://github.com/OpenLineage/OpenLineage/pull/2106) [@tatiana](https://github.com/tatiana)  
    *As of dbt v1.5, usage of target-path in the dbt_project.yml file has been deprecated, now preferring a CLI flag or env var. It will be removed in a future version. This allows users to run `DbtLocalArtifactProcessor` in dbt projects that do not declare target-path.*
* **Proxy: fix Proxy chart** [`#2091`](https://github.com/OpenLineage/OpenLineage/pull/2091) [@harels](https://github.com/harels)  
    *Includes the proper image to deploy in the helm chart.*
* **Python: fix serde filtering** [`#2044`](https://github.com/OpenLineage/OpenLineage/pull/2044) [@xli-1026](https://github.com/xli-1026)  
    *Fixes the bug causing values in list objects to be filtered accidentally.*
* **Python: use non-deprecated `apiKey` if loading it from env variables** [`#2029`](https://github.com/OpenLineage/OpenLineage/pull/2029) [@mobuchowski](https://github.com/mobuchowski)  
    *Changes `api_key` to `apiKey` in `create_token_provider`.*
* **Spark: Improve RDDs on S3 integration.** [`#2039`](https://github.com/OpenLineage/OpenLineage/pull/2039) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Prepares integration test to access S3, fixes input dataset duplicates and includes other minor fixes.*
* **Flink: prevent sending `running` events after job completes** [`#2075`](https://github.com/OpenLineage/OpenLineage/pull/2075) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Flink checkpoint tracking thread was not getting stopped properly on job complete.*
* **Spark & Flink: Unify dataset naming from URI objects** [`#2083`](https://github.com/OpenLineage/OpenLineage/pull/2083) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Makes sure Spark and Flink generate same dataset identifiers for the same datasets by having a single implementation to generate dataset namespace and name.*
* **Spark: Databricks improvements** [`#2076`](https://github.com/OpenLineage/OpenLineage/pull/2076) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Filters unwanted events on databricks and adds an integration test to verify this. Adds integration tests to verify dataset naming on databricks runtime is correct when table location is specified. Adds integration test for wide transformation on delta tables.*

### Removed
* **SQL: remove sqlparser dependency from iface-java and iface-py** [`#2090`](https://github.com/OpenLineage/OpenLineage/pull/2090) [@JDarDagran](https://github.com/JDarDagran)  
   *Removes the dependency due to a breaking change in the latest release of the parser.*

## [1.1.0](https://github.com/OpenLineage/OpenLineage/compare/1.0.0...1.1.0) - 2023-08-23
### Added
* **Flink: create Openlineage configuration based on Flink configuration** [`#2033`](https://github.com/OpenLineage/OpenLineage/pull/2033) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Flink configuration entries starting with `openlineage.*` are passed to the Openlineage client.*
* **Java: add Javadocs to the Java client** [`#2004`](https://github.com/OpenLineage/OpenLineage/pull/2004) [@julienledem](https://github.com/julienledem)  
    *The client was missing some Javadocs.*
* **Spark: append output dataset name to a job name** [`#2036`](https://github.com/OpenLineage/OpenLineage/pull/2036) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Solves problem of multiple jobs, writing to different datasets while having the same job name. The feature is enabled by default and results in different job names and can be disabled by setting `spark.openlineage.jobName.appendDatasetName` to `false`.*
    *Unifies job names generated on the Databricks platform (using a dot job part separator instead of an underscore). The default behaviour can be altered with `spark.openlineage.jobName.replaceDotWithUnderscore`.*
* **Spark: support Spark 3.4.1** [`#2057`](https://github.com/OpenLineage/OpenLineage/pull/2057) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Bumps the latest Spark version to be covered in integration tests.*

### Fixed
* **Airflow: do not use database as fallback when no schema parsed** [`#2023`](https://github.com/OpenLineage/OpenLineage/pull/2023) [@mobuchowski](https://github.com/mobuchowski)  
    *Sets the schema to `None` in `TablesHierarchy` to skip filtering on the schema level in the information schema query.*
* **Flink: fix a bug when getting schema for `KafkaSink`** [`#2042`](https://github.com/OpenLineage/OpenLineage/pull/2042) [@pentium3](https://github.com/pentium3)  
   *Fixes the incomplete schema from `KafkaSinkVisitor` by changing the `KafkaSinkWrapper` to catch schemas of type `AvroSerializationSchema`.*
* **Spark: filter `CreateView` events** [`#1968`](https://github.com/OpenLineage/OpenLineage/pull/1968)[`#1987`](https://github.com/OpenLineage/OpenLineage/pull/1987) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Clears events generated by logical plans having `CreateView` nodes as root.*
* **Spark: fix `MERGE INTO` for delta tables identified by physical locations** [`#2026`](https://github.com/OpenLineage/OpenLineage/pull/2026)
    *Delta tables identified by physical locations were not properly recognized.*
* **Spark: fix incorrect naming of JDBC datasets** [`#2035`](https://github.com/OpenLineage/OpenLineage/pull/2035) [@mobuchowski](https://github.com/mobuchowski)  
    *Makes the namespace generated by the JDBC/Spark connector conform to the naming schema in the spec.*  
* **Spark: fix ignored event `adaptive_spark_plan` in Databricks** [`#2061`](https://github.com/OpenLineage/OpenLineage/pull/2061) [@algorithmy1](https://github.com/algorithmy1)  
    *Removes `adaptive_spark_plan` from the `excludedNodes` in `DatabricksEventFilter`.*

## [1.0.0](https://github.com/OpenLineage/OpenLineage/compare/0.30.1...1.0.0) - 2023-08-01
### Added
* **Airflow: convert lineage from legacy `File` definition** [`#2006`](https://github.com/OpenLineage/OpenLineage/pull/2006) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds coverage for `File` entity definition to enhance backwards compatibility.*

### Removed
* **Spec: remove facet ref from core** [`#1997`](https://github.com/OpenLineage/OpenLineage/pull/1997) [@JDarDagran](https://github.com/JDarDagran)  
    *Removes references to facets from the core spec that broke compatibility with JSON schema specification.*

### Changed 
* **Airflow: change log level to `DEBUG` when extractor isn't found** [`#2012`](https://github.com/OpenLineage/OpenLineage/pull/2012) [@kaxil](https://github.com/kaxil)  
    *Changes log level from `WARNING` to `DEBUG` when an extractor is not available.*
* **Airflow: make sure we cannot fail in thread despite direct execution** [`#2010`](https://github.com/OpenLineage/OpenLineage/pull/2010) [@mobuchowski](https://github.com/mobuchowski)  
    *Ensures the listener is not failing tasks, even in unlikely scenarios.*

### Fixed
* **Airflow: stop using reusable session by default, do not send full event on Snowflake complete** [`#2025`](https://github.com/OpenLineage/OpenLineage/pull/2025) [@mobuchowski](https://github.com/mobuchowski)  
    *Fixes the issue of the Snowflake connector clashing with `HttpTransport` by disabling automatic `requests` session reuse and not running `SnowflakeExtractor` again on job completion.*
* **Client: fix error message to avoid confusion** [`#2001`](https://github.com/OpenLineage/OpenLineage/pull/2001) [@mars-lan](https://github.com/mars-lan)  
    *Fixes the error message in `HttpTransport` in the case of a null URL.*

## [0.30.1](https://github.com/OpenLineage/OpenLineage/compare/0.29.2...0.30.1) - 2023-07-25
### Added
* **Flink: support Iceberg sinks** [`#1960`](https://github.com/OpenLineage/OpenLineage/pull/1960) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Detects output datasets when using an Iceberg table as a sink.*
* **Spark: column-level lineage for `merge into` on delta tables** [`#1958`](https://github.com/OpenLineage/OpenLineage/pull/1958) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Makes column-level lineage support `merge into` on Delta tables. Also refactors column-level lineage to deal with multiple Spark versions.*
* **Spark: column-level lineage for `merge into` on Iceberg tables** [`#1971`](https://github.com/OpenLineage/OpenLineage/pull/1971) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Makes column-level lineage support `merge into` on Iceberg tables.*
* **Spark: add support for Iceberg REST catalog** [`#1963`](https://github.com/OpenLineage/OpenLineage/pull/1963) [@juancappi](https://github.com/juancappi)  
    *Adds `rest` to the existing options of `hive` and `hadoop` in `IcebergHandler.getDatasetIdentifier()` to add support for Iceberg's `RestCatalog`.*
* **Airflow: add possibility to force direct-execution based on environment variable** [`#1934`](https://github.com/OpenLineage/OpenLineage/pull/1934) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds the option to use the direct-execution method on the Airflow listener when the existence of a non-SQLAlchemy-based Airflow event mechanism is confirmed. This happens when using Airflow 2.6 or when the `OPENLINEAGE_AIRFLOW_ENABLE_DIRECT_EXECUTION` environment variable exists.*
* **SQL: add support for Apple Silicon to `openlineage-sql-java`** [`#1981`](https://github.com/OpenLineage/OpenLineage/pull/1981) [@davidjgoss](https://github.com/davidjgoss)  
    *Expands the OS/architecture checks when compiling to produce a specific file for Apple Silicon. Also expands the corresponding OS/architecture checks when loading the binary at runtime from Java code.*
* **Spec: add facet deletion** [`#1975`](https://github.com/OpenLineage/OpenLineage/pull/1975) [@julienledem](https://github.com/julienledem)  
    *In order to add a mechanism for deleting job and dataset facets, adds a `{ _deleted: true }` object that can take the place of any job or dataset facet (but not run or input/output facets, which are valid only for a specific run).*
* **Client: add a file transport** [`#1891`](https://github.com/OpenLineage/OpenLineage/pull/1891) [@Alexkuva](https://github.com/Alexkuva)  
    *Creates a `FileTransport` and its configuration classes supporting append mode or write-new-file mode, which is especially useful when an object store does not support append mode, e.g. in the case of Databricks DBFS FUSE.*

### Changed
* **Airflow: do not run plugin if OpenLineage provider is installed** [`#1999`](https://github.com/OpenLineage/OpenLineage/pull/1999) [@JDarDagran](https://github.com/JDarDagran)  
    *Sets `OPENLINEAGE_DISABLED` to `true` if the provider is installed.*
* **Python: rename `config` to `config_class`** [`#1998`](https://github.com/OpenLineage/OpenLineage/pull/1998) [@mobuchowski](https://github.com/mobuchowski)  
    *Renames the `config` class variable to `config_class` to avoid potential conflict with the config instance.*

### Fixed
* **Airflow: add workaround for airflow-sqlalchemy event mechanism bug** [`#1959`](https://github.com/OpenLineage/OpenLineage/pull/1959) [@mobuchowski](https://github.com/mobuchowski)  
    *Due to known issues with the fork and thread model in the Airflow-SQLAlchemy-based event-delivery mechanism, a Kafka producer left alone does not emit a `COMPLETE`` event. This creates a producer for each event when we detect that we're under Airflow 2.3 - 2.5.*
* **Spark: fix custom environment variables facet** [`#1973`](https://github.com/OpenLineage/OpenLineage/pull/1973) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Enables sending the Spark environment variables facet in a non-deterministic way.*
* **Spark: filter unwanted Delta events** [`#1968`](https://github.com/OpenLineage/OpenLineage/pull/1968) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Clears events generated by logical plans having `Project` node as root.*
* **Python: allow modification of `openlineage.*` logging levels via environment variables** [`#1974`](https://github.com/OpenLineage/OpenLineage/pull/1974) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds `OPENLINEAGE_{CLIENT/AIRFLOW/DBT}_LOGGING` environment variables that can be set according to module logging levels and cleans up some logging calls in `openlineage-airflow`.*

## [0.29.2](https://github.com/OpenLineage/OpenLineage/compare/0.28.0...0.29.2) - 2023-06-30
### Added
* **Flink: support Flink version 1.17.1** [`#1947`](https://github.com/OpenLineage/OpenLineage/pull/1947) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for Flink versions 1.15.4, 1.16.2 and 1.17.1.*
* **Spark: support Spark 3.4** [`#1790`](https://github.com/OpenLineage/OpenLineage/pull/1790) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Introduces support for latest Spark version 3.4.0, along with 3.2.4 and 3.3.2.*
* **Spark: add Databricks platform integration test** [`#1928`](https://github.com/OpenLineage/OpenLineage/pull/1928) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a Spark integration test to verify behavior on Databricks to be run manually in CircleCI when needed.*
* **Spec: add static lineage event types** [`#1880`](https://github.com/OpenLineage/OpenLineage/pull/1880) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *As a first step in implementing static lineage, this adds new `DatasetEvent` and `JobEvent` types to the spec, along with support for the new types in the Python client.*

### Removed
* **Proxy: remove unused Golang client approach** [`#1926`](https://github.com/OpenLineage/OpenLineage/pull/1926) [@mobuchowski](https://github.com/mobuchowski)  
    *Removes the unused Golang proxy, rendered redundant by the fluentd proxy.*
* **Req: bump minimum supported Python version to 3.8** [`#1950`](https://github.com/OpenLineage/OpenLineage/pull/1950) [@mobuchowski](https://github.com/mobuchowski)  
    *Python 3.7 is at EOL. This bumps the minimum supported version to 3.8 to keep the project aligned with the Python EOL schedule.*

### Fixed
* **Flink: fix `KafkaSource` with `GenericRecord`** [`#1944`](https://github.com/OpenLineage/OpenLineage/pull/1944) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Extract dataset schema from `KafkaSource` when `GenericRecord` deserialized is used.*
* **dbt: fix security vulnerabilities** [`#1945`](https://github.com/OpenLineage/OpenLineage/pull/1945) [@JDarDagran](https://github.com/JDarDagran)  
    *Fixes vulnerabilities in the dbt integration and integration tests.*

## [0.28.0](https://github.com/OpenLineage/OpenLineage/compare/0.27.2...0.28.0) - 2023-06-12
### Added
* **dbt: add Databricks compatibility** [`#1829`](https://github.com/OpenLineage/OpenLineage/pull/1829) [Ines70](https://github.com/Ines70)  
    *Enables launching OpenLineage with a Databricks profile.*

### Fixed
* **Fix type-checked marker and packaging** [`#1913`](https://github.com/OpenLineage/OpenLineage/pull/1913) [gaborbernat](https://github.com/gaborbernat)  
    *The client was not marking itself as type-annotated.*
* **Python client: add `schemaURL` to run event** [`#1917`](https://github.com/OpenLineage/OpenLineage/pull/1917) [gaborbernat](https://github.com/gaborbernat)
    *Adds the missing `schemaURL` to the client's `RunState` class.*

## [0.27.2](https://github.com/OpenLineage/OpenLineage/compare/0.27.1...0.27.2) - 2023-06-06
### Fixed
* **Python client: deprecate `client.from_environment`, do not skip loading config** [`#1908`](https://github.com/OpenLineage/OpenLineage/pull/1908) [@mobuchowski](https://github.com/mobuchowski)  
    *Deprecates the `OpenLineage.from_environment` method and recommends using the constructor instead.*

## [0.27.1](https://github.com/OpenLineage/OpenLineage/compare/0.26.0...0.27.1) - 2023-06-05
### Added
* **Python client: add emission filtering mechanism and exact, regex filters** [`#1878`](https://github.com/OpenLineage/OpenLineage/pull/1878) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds configurable job-name filtering to the Python client. Filters can be exact-match- or regex-based. Events will not be sent in the case of matches.*

### Fixed
* **Spark: fix column lineage for aggregate queries on databricks** [`#1867`](https://github.com/OpenLineage/OpenLineage/pull/1867) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Aggregate queries on databricks did not return column lineage.*
* **Airflow: fix unquoted `[` and `]` in Snowflake URIs** [`#1883`](https://github.com/OpenLineage/OpenLineage/pull/1883) [@JDarDagran](https://github.com/JDarDagran)  
    *Snowflake connections containing one of `[` or `]` were causing `urllib.parse.urlparse` to fail.*

## [0.26.0](https://github.com/OpenLineage/OpenLineage/compare/0.25.0...0.26.0) - 2023-05-18
### Added
* **Proxy: Fluentd proxy support (experimental)** [`#1757`](https://github.com/OpenLineage/OpenLineage/pull/1757) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a Fluentd data collector as a proxy to buffer Openlineage events and send them to multiple backends (among many other purposes). Also implements a Fluentd Openlineage parser to validate incoming HTTP events at the beginning of the pipeline. See the [readme](https://github.com/OpenLineage/OpenLineage/tree/main/proxy/fluentd) file for more details.*

### Changed
* **Python client: use Hatchling over setuptools to orchestrate Python env setup** [`#1856`](https://github.com/OpenLineage/OpenLineage/pull/1856) [@gaborbernat](https://github.com/gaborbernat)  
    *Replaces setuptools with Hatchling for building the backend. Also includes a number of fixes, including to type definitions in `transport` and elsewhere.*

### Fixed
* **Spark: support single file datasets** [`#1855`](https://github.com/OpenLineage/OpenLineage/pull/1855) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes the naming of single file datasets so they are no longer named using the parent directory's path: `spark.read.csv('file.csv')`.*
* **Spark: fix `logicalPlan` serialization issue on Databricks** [`#1858`](https://github.com/OpenLineage/OpenLineage/pull/1858) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)   
    *Disables the `spark_unknown` facet by default to turn off serialization of `logicalPlan`.*

## [0.25.0](https://github.com/OpenLineage/OpenLineage/compare/0.24.0...0.25.0) - 2023-05-15
### Added
* **Spark: add Spark/Delta `merge into` support** [`#1823`](https://github.com/OpenLineage/OpenLineage/pull/1823) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for `merge into` queries.*

### Fixed
* **Spark: fix JDBC query handling** [`#1808`](https://github.com/OpenLineage/OpenLineage/pull/1808) [@nataliezeller1](https://github.com/nataliezeller1)  
    *Makes query handling more tolerant of variations in syntax and formatting.*
* **Spark: filter Delta adaptive plan events** [`#1830`](https://github.com/OpenLineage/OpenLineage/pull/1830) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Extends the `DeltaEventFilter` class to filter events in cases where rewritten queries in adaptive Spark plans generate extra events.*
* **Spark: fix Java class cast exception** [`#1844`](https://github.com/OpenLineage/OpenLineage/pull/1844) [@Anirudh181001](https://github.com/Anirudh181001)  
    *Fixes the error caused by the `OpenLineageRunEventBuilder` when it cast the Spark scheduler's `ShuffleMapStage` to boolean.*
* **Flink: include missing fields of Openlineage events** [`#1840`](https://github.com/OpenLineage/OpenLineage/pull/1840) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
    *Enriches Flink events so that missing `eventTime`, `runId` and `job` elements no longer produce errors.*

## [0.24.0](https://github.com/OpenLineage/OpenLineage/compare/0.23.0...0.24.0) - 2023-05-02
### Added
* **Support custom transport types** [`#1795`](https://github.com/OpenLineage/OpenLineage/pull/1795) [@nataliezeller1](https://github.com/nataliezeller1)  
    *Adds a new interface, `TransportBuilder`, for creating custom transport types without having to modify core components of OpenLineage.*
* **Airflow: dbt Cloud integration** [`#1418`](https://github.com/OpenLineage/OpenLineage/pull/1418) [@howardyoo](https://github.com/howardyoo)  
    *Adds a new OpenLineage extractor for dbt Cloud that uses the dbt Cloud hook provided by Airflow to communicate with dbt Cloud via its API.*
* **Spark: support dataset name modification using regex** [`#1796`](https://github.com/OpenLineage/OpenLineage/pull/1796) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
    *It is a common scenario to write Spark output datasets with a location path ending with `/year=2023/month=04`. The Spark parameter `spark.openlineage.dataset.removePath.pattern` introduced here allows for removing certain elements from a path with a regex pattern.*
* **Spark: filter adaptive plan events** [`#1830`](https://github.com/OpenLineage/OpenLineage/pull/1830) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
    *When spark plan is optimized, it is rewritten into adaptive plan which lead to duplicate Openlineage events: per normal and per adaptive plan. This changes filters the latter one.*

### Fixed
* **Spark: catch exception when trying to obtain details of non-existing table.** [`#1798`](https://github.com/OpenLineage/OpenLineage/pull/1798) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
    *This mostly happens when getting table details on START event while the table is still not created.*
* **Spark: LogicalPlanSerializer** [`#1792`](https://github.com/OpenLineage/OpenLineage/pull/1792) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Changes `LogicalPlanSerializer` to make use of non-shaded Jackson classes in order to serialize `LogicalPlans`. Note: class names are no longer serialized.* 
* **Flink: fix Flink CI** [`#1801`](https://github.com/OpenLineage/OpenLineage/pull/1801) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Specifies an older image version that succeeds on CI in order to fix the Flink integration.*

## [0.23.0](https://github.com/OpenLineage/OpenLineage/compare/0.22.0...0.23.0) - 2023-04-20
### Added
* **SQL: parser improvements to support: `copy into`, `create stage`, `pivot`** [`#1742`](https://github.com/OpenLineage/OpenLineage/pull/1742) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for additional syntax available in sqlparser-rs.*
* **dbt: add support for snapshots** [`#1787`](https://github.com/OpenLineage/OpenLineage/pull/1787) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds support for this special kind of table representing type-2 Slowly Changing Dimensions.*

### Changed
* **Spark: change custom column lineage visitors** [`#1788`](https://github.com/OpenLineage/OpenLineage/pull/1788) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Makes the `CustomColumnLineageVisitor` interface public to support custom column lineage.*

### Fixed
* **Spark: fix null pointer in `JobMetricsHolder`** [`#1786`](https://github.com/OpenLineage/OpenLineage/pull/1786) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds a null check before running `put` to fix a NPE occurring in `JobMetricsHolder`*
* **SQL: fix query with table generator** [`#1783`](https://github.com/OpenLineage/OpenLineage/pull/1783) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Allows `TableFactor::TableFunction` to support queries containing table functions.*
* **SQL: fix rust code style bug** [`#1785`](https://github.com/OpenLineage/OpenLineage/pull/1785) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Fixes a minor style issue in `visitor.rs`.*

### Removed
* **Airflow: Remove explicit `pass` from several `extract_on_complete` methods** [`#1771`](https://github.com/OpenLineage/OpenLineage/pull/1771) [JDarDagran](https://github.com/JDarDagran)  
    *Removes the code from three extractors.*

## [0.22.0](https://github.com/OpenLineage/OpenLineage/compare/0.21.1...0.22.0) - 2023-04-03
### Added
* **Spark: properties facet** [`#1717`](https://github.com/OpenLineage/OpenLineage/pull/1717) [@tnazarew](https://github.com/tnazarew)    
    *Adds a new facet to capture specified Spark properties.*
* **SQL: SQLParser supports `alter`, `truncate` and `drop` statements** [`#1695`](https://github.com/OpenLineage/OpenLineage/pull/1695) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for the statements to the parser.*
* **Common/SQL: provide public interface for openlineage_sql package** [`#1727`](https://github.com/OpenLineage/OpenLineage/pull/1727) [@JDarDagran](https://github.com/JDarDagran)  
    *Provides a `.pyi` public interface file for providing typing hints.*
* **Java client: add configurable headers to HTTP transport** [`#1718`](https://github.com/OpenLineage/OpenLineage/pull/1718) [@tnazarew](https://github.com/tnazarew)    
    *Adds custom header handling to `HttpTransport` and the Spark integration.*
* **Python client: create client from dictionary** [`#1745`](https://github.com/OpenLineage/OpenLineage/pull/1745) [@JDarDagran](https://github.com/JDarDagran)  
    *Adds a new `from_dict` method to the Python client to support creating it from a dictionary.*

### Changed
* **Spark: remove URL parameters for JDBC namespaces** [`#1708`](https://github.com/OpenLineage/OpenLineage/pull/1708) [@tnazarew](https://github.com/tnazarew)      
    *Makes the namespace value from an event conform to the naming convention specified in* [Naming.md](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md).
* **Airflow: make `OPENLINEAGE_DISABLED` case-insensitive** [`#1705`](https://github.com/OpenLineage/OpenLineage/pull/1705) [@jedcunningham](https://github.com/jedcunningham)  
    *Makes the environment variable for disabling OpenLineage in the Python client and Airflow integration case-insensitive.*

### Fixed
* **Spark: fix missing BigQuery class in column lineage** [`#1698`](https://github.com/OpenLineage/OpenLineage/pull/1698) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *The Spark integration now checks if the BigQuery classes are available on the classpath before attempting to use them.*
* **DBT: throw `UnsupportedDbtCommand` when finding unsupported entry in `args.which`** [`#1724`](https://github.com/OpenLineage/OpenLineage/pull/1724) [@JDarDagran](https://github.com/JDarDagran)  
    *Adjusts the `dbt-ol` script to detect DBT commands in `run_results.json` only.*

### Removed
* **Spark: remove unnecessary warnings for column lineage** [`#1700`](https://github.com/OpenLineage/OpenLineage/pull/1700) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Removes the warnings about `OneRowRelation` and `LocalRelation` nodes.*
* **Spark: remove deprecated configs** [`#1711`](https://github.com/OpenLineage/OpenLineage/pull/1711) [@tnazarew](https://github.com/tnazarew)    
    *Removes support for deprecated configs.*

## [0.21.1](https://github.com/OpenLineage/OpenLineage/compare/0.20.6...0.21.1) - 2023-03-02
### Added
* **Clients: add `DEBUG` logging of events to transports** [`#1633`](https://github.com/OpenLineage/OpenLineage/pull/1633) [@mobuchowski](https://github.com/mobuchowski)  
    *Ensures that the `DEBUG` loglevel on properly configured loggers will always log events, regardless of the chosen transport.*
* **Spark: add `CustomEnvironmentFacetBuilder` class** [`#1545`](https://github.com/OpenLineage/OpenLineage/pull/1545) ***New contributor*** [@Anirudh181001](https://github.com/Anirudh181001)  
    *Enables the capture of custom environment variables from Spark.*
* **Spark: introduce the new output visitors `AlterTableAddPartitionCommandVisitor` and `AlterTableSetLocationCommandVisitor`** [`#1629`](https://github.com/OpenLineage/OpenLineage/pull/1629) ***New contributor*** [@nataliezeller1](https://github.com/nataliezeller1)  
    *Adds visitors for extracting table names from the Spark commands `AlterTableAddPartitionCommand` and `AlterTableSetLocationCommand`. The intended use case is a custom transport for the OpenMetadata lineage API.*
* **Spark: add column lineage for JDBC relations** [`#1636`](https://github.com/OpenLineage/OpenLineage/pull/1636) [@tnazarew](https://github.com/tnazarew)  
    *Adds column lineage information to JDBC events with data extracted from query by the SQL parser.*
* **SQL: add linux-aarch64 native library to Java SQL parser** [`#1664`](https://github.com/OpenLineage/OpenLineage/pull/1664) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds a Linux-ARM version of the native library. The Java SQL parser interface had only Linux-x64 and MacOS universal binary variants previously.*

### Changed
* **Airflow: get table database in Athena extractor** [`#1631`](https://github.com/OpenLineage/OpenLineage/pull/1631) ***New contributor*** [@rinzool](https://github.com/rinzool)  
    *Changes the extractor to get a table's database from the `table.schema` field or the operator default if the field is `None`.*

### Fixed
* **dbt: add dbt `seed` to the list of dbt-ol events** [`#1649`](https://github.com/OpenLineage/OpenLineage/pull/1649) ***New contributor*** [@pohek321](https://github.com/pohek321)  
    *Ensures that `dbt-ol test` no longer fails when run against an event seed.*
* **Spark: make column lineage extraction in Spark support caching** [`#1634`](https://github.com/OpenLineage/OpenLineage/pull/1634) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Collect column lineage from Spark logical plans that contain cached datasets.*
* **Spark: add support for a deprecated config** [`#1586`](https://github.com/OpenLineage/OpenLineage/pull/1586) [@tnazarew](https://github.com/tnazarew)  
    *Maps the deprecated `spark.openlineage.url` to `spark.openlineage.transport.url`.*
* **Spark: add error message in case of null in url** [`#1590`](https://github.com/OpenLineage/OpenLineage/pull/1590) [@tnazarew](https://github.com/tnazarew)  
    *Improves error logging in the case of undefined URLs.*
* **Spark: collect complete event for really quick Spark jobs** [`#1650`](https://github.com/OpenLineage/OpenLineage/pull/1650) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)   
    *Improves the collecting of OpenLineage events on SQL complete in the case of quick operations.*
* **Spark: fix input/outputs for one node `LogicalRelation` plans** [`#1668`](https://github.com/OpenLineage/OpenLineage/pull/1668) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *For simple queries like `select col1, col2 from my_db.my_table` that do not write output, 
    the Spark plan contained just a single node, which was wrongly treated as both 
    an input and output dataset.*
* **SQL: fix file existence check in build script for openlineage-sql-java** [`#1613`](https://github.com/OpenLineage/OpenLineage/pull/1613) [@sekikn](https://github.com/sekikn)  
    *Ensures that the build script works if the library is compiled solely for Linux.*

### Removed
* **Airflow: remove `JobIdMapping` and update macros to better support Airflow version 2+** [`#1645`](https://github.com/OpenLineage/OpenLineage/pull/1645) [@JDarDagran](https://github.com/JDarDagran)  
    *Updates macros to use `OpenLineageAdapter`'s method to generate deterministic run UUIDs because using the `JobIdMapping` utility is incompatible with Airflow 2+.*

### Added

* Spark: column lineage for JDBC relations [`#1636`](https://github.com/OpenLineage/OpenLineage/pull/1636) [@tnazarew](https://github.com/tnazarew)
  * Adds column lineage info to JDBC events with data extracted form query by OL SQL parser

## [0.20.6](https://github.com/OpenLineage/OpenLineage/compare/0.20.4...0.20.6) - 2023-02-10
### Added
* Airflow: add new extractor for `FTPFileTransmitOperator` [`#1603`](https://github.com/OpenLineage/OpenLineage/pull/1601) [@sekikn](https://github.com/sekikn)  
    *Adds a new extractor for this Airflow operator serving legacy systems.*

### Changed
* Airflow: make extractors for async operators work [`#1601`](https://github.com/OpenLineage/OpenLineage/pull/1601) [@JDarDagran](https://github.com/JDarDagran)  
    *Sends a deterministic Run UUID for Airflow runs.*

### Fixed
* dbt: render actual profile only in profiles.yml [`#1599`](https://github.com/OpenLineage/OpenLineage/pull/1599) [@mobuchowski](https://github.com/mobuchowski)  
    *Adds an `include_section` argument for the Jinja render method to include only one profile if needed.*
* dbt: make `compiled_code` optional [`#1595`](https://github.com/OpenLineage/OpenLineage/pull/1595) [@JDarDagran](https://github.com/JDarDagran)  
    *Makes `compiled_code` optional for manifest > v7.*

## [0.20.4](https://github.com/OpenLineage/OpenLineage/compare/0.19.2...0.20.4) - 2023-02-07
### Added
* Airflow: add new extractor for `GCSToGCSOperator` [`#1495`](https://github.com/OpenLineage/OpenLineage/pull/1495) [@sekikn](https://github.com/sekikn)  
    *Adds a new extractor for this operator.*
* Flink: resolve topic names from regex, support 1.16.0 [`#1522`](https://github.com/OpenLineage/OpenLineage/pull/1522) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)  
    *Adds support for Flink 1.16.0 and makes the integration resolve topic names from Kafka topic patterns.*
* Proxy: implement lineage event validator for client proxy [`#1469`](https://github.com/OpenLineage/OpenLineage/pull/1469) [@fm100](https://github.com/fm100)  
    *Implements logic in the proxy (which is still in development) for validating and handling lineage events.*

### Changed
* CI: use `ruff` instead of flake8, isort, etc., for linting and formatting [`#1526`](https://github.com/OpenLineage/OpenLineage/pull/1526) [@mobuchowski](https://github.com/mobuchowski)  
    *Adopts the `ruff` package, which combines several linters and formatters into one fast binary.*

### Fixed
* Airflow: make the Trino catalog non-mandatory [`#1572`](https://github.com/OpenLineage/OpenLineage/pull/1572) [@JDarDagran](https://github.com/JDarDagran)  
    *Makes the Trino catalog optional in the Trino extractor.*
* Common: add explicit SQL dependency [`#1532`](https://github.com/OpenLineage/OpenLineage/pull/1532) [@mobuchowski](https://github.com/mobuchowski)  
    *Addresses a 0.19.2 breaking change to the GE integration by including the SQL dependency explicitly.*
* DBT: adjust `tqdm` logging in `dbt-ol` [`#1549`](https://github.com/OpenLineage/OpenLineage/pull/1549) [@JdarDagran](https://github.com/JDarDagran)  
    *Adjusts `tqdm` to show the correct number of iterations and adds START events for parent runs.*
* DBT: fix typo in log output [`#1493`](https://github.com/OpenLineage/OpenLineage/pull/1493) [@denimalpaca](https://github.com/denimalpaca)  
    *Fixes 'emittled' typo in log output.*
* Great Expectations/Airflow: follow Snowflake dataset naming rules [`#1527`](https://github.com/OpenLineage/OpenLineage/pull/1527) [@mobuchowski](https://github.com/mobuchowski)  
    *Normalizes Snowflake dataset and datasource naming rules among DBT/Airflow/GE; canonizes old Snowflake account paths around making them all full-size with account, region and cloud names.*
* Java and Python Clients: Kafka does not initialize properties if they are empty; check and notify about Confluent-Kafka requirement [`#1556`](https://github.com/OpenLineage/OpenLineage/pull/1556) [@mobuchowski](https://github.com/mobuchowski)  
    *Fixes the failure to initialize `KafkaTransport` in the Java client and adds an exception if the required `confluent-kafka` module is missing from the Python client.*
* Spark: add square brackets for list-based Spark configs [`#1507`](https://github.com/OpenLineage/OpenLineage/pull/1507) [@Varunvaruns9](https://github.com/Varunvaruns9)  
    *Adds a condition to treat configs with `[]` as lists. Note: `[]` will be required for list-based configs starting with 0.21.0.*
* Spark: fix several Spark/BigQuery-related issues [`#1557`](https://github.com/OpenLineage/OpenLineage/pull/1557) [@mobuchowski](https://github.com/mobuchowski)  
    *Fixes the assumption that a version is always a number; adds support for `HadoopMapReduceWriteConfigUtil`; makes the integration access `BigQueryUtil` and `getTableId` using reflection, which supports all BigQuery versions; makes logs provide the full serialized LogicalPlan on `debug`.*
* SQL: only report partial failures [`#1479](https://github.com/OpenLineage/OpenLineage/pull/1479) [@mobuchowski](https://github.com/mobuchowski)  
    *Changes the parser so it reports partial failures instead of failing the whole extraction.*

## [0.19.2](https://github.com/OpenLineage/OpenLineage/compare/0.18.0...0.19.2) - 2023-01-04
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


## [0.18.0](https://github.com/OpenLineage/OpenLineage/compare/0.17.0...0.18.0) - 2022-12-08
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

## [0.16.1](https://github.com/OpenLineage/OpenLineage/compare/0.15.1...0.16.1) - 2022-11-03
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
    *Prevents evaluation of column lineage when the `schemFacet` is `null`.*
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

## [0.9.0](https://github.com/OpenLineage/OpenLineage/compare/0.8.2...0.9.0) - 2022-06-03
### Added
* Spark: Column-level lineage introduced for Spark integration ([#698](https://github.com/OpenLineage/OpenLineage/pull/698), [#645](https://github.com/OpenLineage/OpenLineage/pull/645)) [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Java: Spark to use Java client directly ([#774](https://github.com/OpenLineage/OpenLineage/pull/774)) [@mobuchowski](https://github.com/mobuchowski)
* Clients: Add OPENLINEAGE_DISABLED environment variable which overrides config to NoopTransport ([#780](https://github.com/OpenLineage/OpenLineage/pull/780)) [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* Set log to debug on unknown facet entry ([#766](https://github.com/OpenLineage/OpenLineage/pull/766)) [@wslulciuc](https://github.com/wslulciuc)
* Dagster: pin protobuf version to 3.20 as suggested by tests ([#787](https://github.com/OpenLineage/OpenLineage/pull/787)) [@mobuchowski](https://github.com/mobuchowski)
* Add SafeStrDict to skip failing attributes ([#798](https://github.com/OpenLineage/OpenLineage/pull/798)) [@JDarDagran](https://github.com/JDarDagran)

## [0.8.2](https://github.com/OpenLineage/OpenLineage/compare/0.8.1...0.8.2) - 2022-05-19
### Added
* `openlineage-airflow` now supports getting credentials from [Airflows secrets backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) ([#723](https://github.com/OpenLineage/OpenLineage/pull/723)) [@mobuchowski](https://github.com/mobuchowski)
* `openlineage-spark` now supports [Azure Databricks Credential Passthrough](https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough) ([#595](https://github.com/OpenLineage/OpenLineage/pull/595)) [@wjohnson](https://github.com/wjohnson)
* `openlineage-spark` detects datasets wrapped by `ExternalRDD`s ([#746](https://github.com/OpenLineage/OpenLineage/pull/746)) [@collado-mike](https://github.com/collado-mike)

### Fixed
* `PostgresOperator` fails to retrieve host and conn during extraction ([#705](https://github.com/OpenLineage/OpenLineage/pull/705)) [@sekikn](https://github.com/sekikn)
* SQL parser accepts lists of sql statements ([#734](https://github.com/OpenLineage/OpenLineage/issues/734)) [@mobuchowski](https://github.com/mobuchowski)
* Missing schema when writing to Delta tables in Databricks ([#748](https://github.com/OpenLineage/OpenLineage/pull/748)) [@collado-mike](https://github.com/collado-mike)

## [0.8.1](https://github.com/OpenLineage/OpenLineage/compare/0.7.1...0.8.1) - 2022-04-29
### Added
* Airflow integration uses [new TaskInstance listener API](https://github.com/apache/airflow/blob/main/docs/apache-airflow/listeners.rst) for Airflow 2.3+ ([#508](https://github.com/OpenLineage/OpenLineage/pull/508)) [@mobuchowski](https://github.com/mobuchowski)
* Support for HiveTableRelation as input source in Spark integration ([#683](https://github.com/OpenLineage/OpenLineage/pull/683)) [@collado-mike](https://github.com/collado-mike)
* Add HTTP and Kafka Client to `openlineage-java` lib ([#480](https://github.com/OpenLineage/OpenLineage/pull/480)) [@wslulciuc](https://github.com/wslulciuc), [@mobuchowski](https://github.com/mobuchowski)
* New SQL parser, used by Postgres, Snowflake, Great Expectations integrations ([#644](https://github.com/OpenLineage/OpenLineage/pull/644)) [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* GreatExpectations: Fixed bug when invoking GreatExpectations using v3 API ([#683](https://github.com/OpenLineage/OpenLineage/pull/689)) [@collado-mike](https://github.com/collado-mike)
 
## [0.7.1](https://github.com/OpenLineage/OpenLineage/compare/0.6.2...0.7.1) - 2022-04-19
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

## [0.6.2](https://github.com/OpenLineage/OpenLineage/compare/0.6.1...0.6.2) - 2022-03-16
### Added
* CI: add integration tests for Airflow's SnowflakeOperator and dbt-snowflake [@mobuchowski](https://github.com/mobuchowski)
* Introduce DatasetVersion facet in spec [@pawel-big-lebowski](https://github.com/pawel-big-lebowski)
* Airflow: add external query id facet [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* Complete Fix of Snowflake Extractor get_hook() Bug [@denimalpaca](https://github.com/denimalpaca)
* Update artwork [@rossturk](https://github.com/rossturk)
* Airflow tasks in a DAG now report a common ParentRunFacet [@collado-mike](https://github.com/collado-mike)

## [0.6.1](https://github.com/OpenLineage/OpenLineage/compare/0.6.0...0.6.1) - 2022-03-07
### Fixed
* Catch possible failures when emitting events and log them [@mobuchowski](https://github.com/mobuchowski)

### Fixed
* dbt: jinja2 code using do extensions does not crash [@mobuchowski](https://github.com/mobuchowski)


## [0.6.0](https://github.com/OpenLineage/OpenLineage/compare/0.5.2...0.6.0) - 2022-03-04
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

## [0.5.2](https://github.com/OpenLineage/OpenLineage/compare/0.5.1...0.5.2) - 2022-02-10
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

## [0.5.1](https://github.com/OpenLineage/OpenLineage/compare/0.4.0...0.5.1) - 2022-01-18
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

* **An initial specification.** The the initial version [`1-0-0`](https://github.com/OpenLineage/OpenLineage/blob/0.1.0/spec/OpenLineage.md) of the OpenLineage specification defines the core model and facets.
* **Integrations** that collect lineage metadata as OpenLineage events:
  * [`Apache Airflow`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow) with support for BigQuery, Great Expectations, Postgres, Redshift, Snowflake
  * [`Apache Spark`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark)
  * [`dbt`](https://github.com/OpenLineage/OpenLineage/tree/main/integration/dbt)
* **Clients** that send OpenLineage events to an HTTP backend. Both [`java`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java) and [`python`](https://github.com/OpenLineage/OpenLineage/tree/main/client/python) are initially supported.
