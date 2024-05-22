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
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import io.openlineage.hive.client.HiveOpenLineageConfigParser;
import io.openlineage.hive.hooks.HiveOpenLineageHook;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.LineageLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HiveRunnerExtension.class)
public abstract class IntegrationTestsBase {

  protected static String dataset;
  public static final String GCP_LOCATION_ENV_VAR = "GCP_LOCATION";
  public static final String TEST_BUCKET_ENV_VAR = "TEST_BUCKET";
  protected static String testBucketName;

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
        throw new RuntimeException(e);
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
  public void setupEach() {
    initHive();
  }

  @AfterAll
  static void tearDownAll() {
    // Cleanup the test BQ dataset
    deleteBqDatasetAndTables(dataset);
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

  private static com.google.auth.Credentials getCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new RuntimeException(e);
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
    addExecHooks(HiveConf.ConfVars.POSTEXECHOOKS, HiveOpenLineageHook.class, LineageLogger.class);
    hive.setHiveConfValue(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    hive.setHiveConfValue(HiveOpenLineageConfigParser.CONF_PREFIX + "transport.type", "dummy");
    //    hive.setHiveConfValue(HiveOpenLineageConfigParser.CONF_PREFIX + "transport.type",
    // "datacatalog");
    //    hive.setHiveConfValue(HiveOpenLineageConfigParser.CONF_PREFIX + "transport.project",
    // getProject());
    //    hive.setHiveConfValue(HiveOpenLineageConfigParser.CONF_PREFIX + "transport.location",
    // getGcpLocation());
    hive.setHiveConfValue(HiveOpenLineageConfigParser.CONF_PREFIX + "namespace", "testnamespace");
    hive.setHiveConfValue("hive.tez.container.size", "1024");
    hive.start();
  }

  public String renderQueryTemplate(String queryTemplate) {
    Map<String, Object> params = new HashMap<>();
    params.put("project", getProject());
    params.put("dataset", dataset);
    params.put("location", getGcpLocation());
    return StrSubstitutor.replace(queryTemplate, params, "${", "}");
  }

  public void createHiveTable(
      String tableName, String hiveDDL, boolean isExternal, String properties, String comment) {
    runHiveQuery(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + tableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
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
    createHiveTable(tableName, hiveDDL, false, null, null);
  }

  public void createExternalHiveTable(String tableName, String hiveDDL) {
    createHiveTable(tableName, hiveDDL, true, null, null);
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
      throw new RuntimeException(e);
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
