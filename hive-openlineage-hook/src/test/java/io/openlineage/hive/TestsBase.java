/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.datacatalog.lineage.v1.LineageEvent;
import com.google.cloud.datacatalog.lineage.v1.Link;
import com.google.cloud.datacatalog.lineage.v1.LocationName;
import com.google.cloud.datacatalog.lineage.v1.Process;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import io.openlineage.hive.hooks.HiveOpenLineageHook;
import io.openlineage.hive.testutils.DataplexTestUtils;
import io.openlineage.hive.testutils.HivePropertiesExtension;
import io.openlineage.hive.transport.DummyTransport;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({HiveRunnerExtension.class, HivePropertiesExtension.class})
public abstract class TestsBase {

  protected static String dataset;
  public static final String GCP_LOCATION_ENV_VAR = "GCP_LOCATION";
  public static final String TEST_BUCKET_ENV_VAR = "TEST_BUCKET";
  protected static String testBucketName;
  protected String olJobNamespace;
  protected String olJobName;
  protected Process dataplexProcess;

  @HiveSQL(
      files = {},
      autoStart = false)
  protected HiveShell hive;

  @BeforeAll
  public static void setUpAll() {
    // Create test bucket
    testBucketName = getTestBucket();
    try {
      createBucket(testBucketName);
    } catch (StorageException e) {
      if (e.getCode() == 409) {
        // The bucket already exists, which is okay.
      } else {
        throw new IllegalStateException(e);
      }
    }

    // Create the test dataset in BigQuery
    dataset =
        String.format(
            "lineage_tests_%d_%d",
            Long.MAX_VALUE - System.currentTimeMillis(), Long.MAX_VALUE - System.nanoTime());
    createBqDataset(dataset);
  }

  @BeforeEach
  public void setupEach(TestInfo testInfo) {
    DummyTransport.getEvents().clear();
    String className = testInfo.getTestClass().map(Class::getSimpleName).orElse("UnknownClass");
    String methodName = testInfo.getTestMethod().map(Method::getName).orElse("UnknownMethod");
    olJobName = String.format("%s.%s", className, methodName);
    olJobNamespace =
        String.format(
            "test_namespace_%d_%d",
            Long.MAX_VALUE - System.currentTimeMillis(), Long.MAX_VALUE - System.nanoTime());
    initHive();
  }

  @AfterAll
  static void tearDownAll() {
    // Cleanup the test BQ dataset
    deleteBqDatasetAndTables(dataset);
  }

  @AfterEach
  public void tearDownEach(TestInfo testInfo) throws IOException {
    Process process = getDataplexProcess();
    if (process != null) {
      DataplexTestUtils.deleteProcess(process);
    }
  }

  public Process getDataplexProcess() throws IOException {
    if (dataplexProcess == null) {
      return DataplexTestUtils.getProcessWithDisplayName(
          getDataplexLocationName(), String.format("%s:%s", olJobNamespace, olJobName));
    }
    return dataplexProcess;
  }

  public List<Link> getBigQueryLinks(String sourceTable, String targetTable)
      throws IOException, InterruptedException {
    // FIXME: The Links are created asynchronously in the Dataplex backend so there might be a
    //  small delay before we can retrieve them.
    // TODO: Replace with a more robust retry strategy
    Thread.sleep(2000);
    String source = null;
    String target = null;
    if (sourceTable != null) {
      source = String.format("bigquery:%s.%s.%s", getProject(), dataset, sourceTable);
    }
    if (targetTable != null) {
      target = String.format("bigquery:%s.%s.%s", getProject(), dataset, targetTable);
    }
    return DataplexTestUtils.searchLinks(getDataplexLocationName(), source, target);
  }

  public List<LineageEvent> getDataplexEvents() throws IOException {
    Process process = getDataplexProcess();
    if (process != null) {
      return DataplexTestUtils.getEventsForProcess(process.getName());
    } else {
      return Collections.emptyList();
    }
  }

  public static void createBqDataset(String dataset) {
    DatasetId datasetId = DatasetId.of(dataset);
    getBigQueryService()
        .create(DatasetInfo.newBuilder(datasetId).setLocation(getGcpLocation()).build());
  }

  public static void deleteBqDatasetAndTables(String dataset) {
    getBigQueryService()
        .delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
  }

  public static String getTestBucket() {
    return System.getenv().getOrDefault(TEST_BUCKET_ENV_VAR, getProject() + "-integration-tests");
  }

  public static String getGcpLocation() {
    return System.getenv().getOrDefault(GCP_LOCATION_ENV_VAR, "us");
  }

  public static void createBucket(String bucketName) {
    getStorageClient()
        .create(BucketInfo.newBuilder(bucketName).setLocation(getGcpLocation()).build());
  }

  public static String getProject() {
    return getBigQueryService().getOptions().getProjectId();
  }

  public static LocationName getDataplexLocationName() {
    return LocationName.of(getProject(), getGcpLocation());
  }

  private static Storage getStorageClient() {
    return StorageOptions.newBuilder().setCredentials(getCredentials()).build().getService();
  }

  public static void uploadBlob(String bucketName, String objectName, byte[] contents) {
    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    getStorageClient().create(blobInfo, contents);
  }

  public static BigQuery getBigQueryService() {
    return BigQueryOptions.newBuilder().setCredentials(getCredentials()).build().getService();
  }

  public static com.google.auth.Credentials getCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Add the given hook to the appropriate configuration's pre/post/failure hooks property. */
  public void addExecHooks(
      HiveConf.ConfVars hookType, Class<? extends ExecuteWithHookContext>... hooks) {
    String hooksStr = Arrays.stream(hooks).map(Class::getName).collect(Collectors.joining(","));
    hive.setHiveConfValue(hookType.varname, hooksStr);
  }

  protected static String getDefaultExecutionEngine() {
    return "tez";
  }

  public void initHive() {
    initHive(getDefaultExecutionEngine());
  }

  public void initHive(String engine) {
    addExecHooks(HiveConf.ConfVars.POSTEXECHOOKS, HiveOpenLineageHook.class);
    hive.setHiveConfValue(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    hive.setHiveConfValue("hive.openlineage.transport.type", "composite");
    hive.setHiveConfValue(
        "hive.openlineage.transport.transports",
        "[{\"type\":\"dummy\"}, {\"type\":\"gcplineage\",\"mode\":\"sync\"}]");
    // TODO: Check if this should instead be "hive.openlineage.job.namespace"
    hive.setHiveConfValue("hive.openlineage.namespace", olJobNamespace);
    hive.setHiveConfValue("hive.openlineage.job.name", olJobName);
    hive.setHiveConfValue("hive.tez.container.size", "1024");

    // Load potential Hive config values passed from the HiveProperties extension
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      hive.setHiveConfValue(key, hiveConfSystemOverrides.get(key));
    }

    hive.start();
  }

  /** Return Hive config values passed from system properties */
  public static Map<String, String> getHiveConfSystemOverrides() {
    Map<String, String> overrides = new HashMap<>();
    Properties systemProperties = System.getProperties();
    for (String key : systemProperties.stringPropertyNames()) {
      if (key.startsWith(HivePropertiesExtension.HIVECONF_SYSTEM_OVERRIDE_PREFIX)) {
        String hiveConfKey =
            key.substring(HivePropertiesExtension.HIVECONF_SYSTEM_OVERRIDE_PREFIX.length());
        overrides.put(hiveConfKey, systemProperties.getProperty(key));
      }
    }
    return overrides;
  }

  public String renderQueryTemplate(String queryTemplate) {
    Map<String, Object> params = new HashMap<>();
    params.put("project", getProject());
    params.put("dataset", dataset);
    params.put("location", getGcpLocation());
    return StrSubstitutor.replace(queryTemplate, params, "${", "}");
  }

  public void createHiveTable(
      String tableName,
      String hiveDDL,
      boolean isExternal,
      String properties,
      String comment,
      String partitionColumn) {
    runHiveQuery(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + tableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
            partitionColumn != null ? "PARTITIONED BY (" + partitionColumn + ")" : "",
            properties != null ? "TBLPROPERTIES (" + properties + ")" : ""));
  }

  public void createHiveBigQueryTable(
      String hmsTableName,
      String bqTableName,
      String hiveDDL,
      boolean isExternal,
      String properties,
      String comment) {
    runHiveQuery(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + hmsTableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
            "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "  'bq.table'='${project}.${dataset}." + bqTableName + "'",
            properties != null ? "," + properties : "",
            ")"));
  }

  public void createGcsCsvTable(
      String hmsTableName, String hiveDDL, String gcsPath, boolean skipFirstLine) {
    runHiveQuery(
        String.join(
            "\n",
            "CREATE EXTERNAL TABLE " + hmsTableName + " (",
            hiveDDL,
            ")",
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'",
            "WITH SERDEPROPERTIES (",
            "    'separatorChar' = ',',",
            "    'quoteChar' = '\\\"'",
            ")",
            "STORED AS TEXTFILE",
            "LOCATION '" + gcsPath + "'",
            skipFirstLine ? "TBLPROPERTIES ('skip.header.line.count'='1')" : ""));
  }

  public void createManagedHiveTable(String tableName, String hiveDDL) {
    createHiveTable(tableName, hiveDDL, false, null, null, null);
  }

  public void createPartitionedHiveTable(String tableName, String hiveDDL, String partitionColumn) {
    createHiveTable(tableName, hiveDDL, true, null, null, partitionColumn);
  }

  public void createExternalHiveBigQueryTable(String tableName, String hiveDDL, String bqDDL) {
    createHiveBigQueryTable(tableName, tableName, hiveDDL, true, null, null);
    createBigQueryTable(tableName, bqDDL);
  }

  public void createBigQueryTable(String tableName, String bqDDL) {
    createBigQueryTable(tableName, bqDDL, null);
  }

  public void createBigQueryTable(String tableName, String bqDDL, String description) {
    runBqQuery(
        String.join(
            "\n",
            "CREATE OR REPLACE TABLE ${dataset}." + tableName,
            "(",
            bqDDL,
            ")",
            description != null ? "OPTIONS ( description=\"" + description + "\" )" : ""));
  }

  public TableResult runBqQuery(String queryTemplate) {
    QueryJobConfiguration jobConfig =
        QueryJobConfiguration.newBuilder(renderQueryTemplate(queryTemplate)).build();
    try {
      return getBigQueryService().query(jobConfig);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Runs a single Hive statement. */
  public List<Object[]> runHiveQuery(String queryTemplate) {
    // Remove the ';' character at the end if there is one
    String cleanedTemplate = StringUtils.stripEnd(queryTemplate, null);
    if (StringUtils.endsWith(queryTemplate, ";")) {
      cleanedTemplate = StringUtils.chop(cleanedTemplate);
    }
    return hive.executeStatement(renderQueryTemplate(cleanedTemplate));
  }
}
