/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEHISTORYFILELOC;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSAUTOGATHER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_CBO_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.LOCALSCRATCHDIR;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_COLUMNS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_CONSTRAINTS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_VALIDATE_TABLES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.SCRATCHDIR;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.hive.hooks.OutputCLL;
import io.openlineage.hive.hooks.TransformationInfo;
import io.openlineage.hive.parsing.ColumnLineageCollector;
import io.openlineage.hive.parsing.Parsing;
import io.openlineage.hive.parsing.QueryExpr;
import io.openlineage.hive.util.HiveUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

public abstract class InMemoryHiveTestBase {

  public static HiveConf hiveConf = new HiveConf();
  public static QueryState queryState;
  private static final Path baseDir =
      Paths.get(System.getProperty("java.io.tmpdir"), "hive-test-" + UUID.randomUUID());

  @BeforeAll
  static void beforeAll() throws IOException {
    Files.createDirectories(baseDir);
    configureMiscHiveSettings();
    configureMetaStore();
    configureFileSystem();
    queryState = new QueryState.Builder().withHiveConf(hiveConf).build();
    SessionState.start(hiveConf);
  }

  @AfterAll
  static void afterAll() throws IOException {
    FileUtils.deleteDirectory(baseDir.toFile());
  }

  @AfterEach
  void afterEach() throws TException {
    deleteAllTables();
  }

  static void configureMiscHiveSettings() {
    hiveConf.setBoolVar(HIVESTATSAUTOGATHER, false);
    // Turn off dependency on calcite library
    hiveConf.setBoolVar(HIVE_CBO_ENABLED, false);
    // Disable to avoid exception when stopping the session
    hiveConf.setBoolVar(HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
  }

  static void configureMetaStore() throws IOException {
    Path derbyLogFile = Files.createTempFile(baseDir, "derby", ".log");
    System.setProperty("derby.stream.error.file", derbyLogFile.toString());
    hiveConf.set(
        METASTORECONNECTURLKEY.varname, "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true");
    hiveConf.set("datanucleus.schema.autoCreateAll", "true");
    hiveConf.set("datanucleus.schema.autoCreateTables", "true");
    hiveConf.set("datanucleus.connectionPoolingType", "None");
    hiveConf.set("hive.metastore.schema.verification", "false");
    hiveConf.set(
        "metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");
    String jdbcDriver = org.apache.derby.jdbc.EmbeddedDriver.class.getName();
    hiveConf.set("datanucleus.connectiondrivername", jdbcDriver);
    hiveConf.set("javax.jdo.option.ConnectionDriverName", jdbcDriver);
    hiveConf.set(HIVE_IN_TEST.getVarname(), "true");
    hiveConf.set(METASTORE_VALIDATE_CONSTRAINTS.varname, "true");
    hiveConf.set(METASTORE_VALIDATE_COLUMNS.varname, "true");
    hiveConf.set(METASTORE_VALIDATE_TABLES.varname, "true");
  }

  static void configureFileSystem() throws IOException {
    createAndSetDirectoryProperty(METASTOREWAREHOUSE, "warehouse");
    createAndSetDirectoryProperty(SCRATCHDIR, "scratchdir");
    createAndSetDirectoryProperty(LOCALSCRATCHDIR, "localscratchdir");
    createAndSetDirectoryProperty(HIVEHISTORYFILELOC, "tmp");
  }

  static Path createDirectory(String dir) throws IOException {
    Path newDir = Files.createTempDirectory(baseDir, dir);
    FileUtil.setPermission(newDir.toFile(), FsPermission.getDirDefault());
    return newDir;
  }

  static void createAndSetDirectoryProperty(HiveConf.ConfVars var, String dir) throws IOException {
    hiveConf.set(var.varname, createDirectory(dir).toAbsolutePath().toString());
  }

  public static void deleteAllTables() throws TException {
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf)) {
      List<String> tables = client.getAllTables("default");
      for (String table : tables) {
        client.dropTable("default", table, true, true);
      }
    }
  }

  public static void createTable(String table, String... fields) throws TException {
    createPartitionedTable(table, null, fields);
  }

  public static void createPartitionedTable(String table, String partitionField, String... fields)
      throws TException {
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf)) {
      Table hiveTable = new Table();
      hiveTable.setDbName("default");
      hiveTable.setTableName(table);
      StorageDescriptor sd = new StorageDescriptor();
      sd.setCols(getTableColumns(fields));
      sd.setLocation(hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE) + table);
      sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
      sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      sd.setSerdeInfo(serdeInfo);
      hiveTable.setSd(sd);
      if (partitionField != null) {
        List<FieldSchema> partitionCols = new ArrayList<>();
        partitionCols.add(getTableColumns(partitionField).get(0));
        hiveTable.setPartitionKeys(partitionCols);
      }
      client.createTable(hiveTable);
    }
  }

  public static List<FieldSchema> getTableColumns(String... fields) {
    List<FieldSchema> cols = new ArrayList<>();
    for (String field : fields) {
      String[] parts = field.split(";");
      if (parts.length == 2) {
        cols.add(new FieldSchema(parts[0], parts[1], ""));
      } else {
        throw new IllegalArgumentException("Invalid field format: " + field);
      }
    }
    return cols;
  }

  public static OutputCLL getOutputCLL(String queryString, String outputTable, String... fields) {
    org.apache.hadoop.hive.ql.metadata.Table mockTable =
        mock(org.apache.hadoop.hive.ql.metadata.Table.class);
    when(mockTable.getFullyQualifiedName()).thenReturn(outputTable);
    when(mockTable.getCols()).thenReturn(getTableColumns(fields));
    SemanticAnalyzer semanticAnalyzer = HiveUtils.analyzeQuery(hiveConf, queryState, queryString);
    QueryExpr queryExpr = Parsing.buildQueryTree(semanticAnalyzer.getQB(), outputTable);
    return ColumnLineageCollector.collectCLL(queryExpr, mockTable);
  }

  public static void assertCountColumnDependencies(OutputCLL outputCLL, int expected) {
    int count = 0;
    for (Map.Entry<String, Map<String, Set<TransformationInfo>>> outputEntry :
        outputCLL.getColumnDependencies().entrySet()) {
      for (Map.Entry<String, Set<TransformationInfo>> transformations :
          outputEntry.getValue().entrySet()) {
        count += transformations.getValue().size();
      }
    }
    assertThat(count).isEqualTo(expected);
  }

  public static void assertCountDatasetDependencies(OutputCLL outputCLL, int expected) {
    int count = 0;
    for (String inputColumn : outputCLL.getDatasetDependencies().keySet()) {
      count += outputCLL.getDatasetDependencies().get(inputColumn).size();
    }
    assertThat(count).isEqualTo(expected);
  }
}
