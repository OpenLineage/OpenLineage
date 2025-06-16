/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.util;

import static io.openlineage.hive.util.HiveUtils.getDatasetIdentifierFromTable;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.jupiter.api.Test;

class HiveUtilsTest {

  @Test
  void testDatasetIdentifierFromSimpleTable() {
    org.apache.hadoop.hive.metastore.api.Table metastoreApiTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    metastoreApiTable.setTableName("mytable");
    metastoreApiTable.setDbName("mydb");
    metastoreApiTable.setCatName("mycatalog");
    Table table = new Table(metastoreApiTable);
    DatasetIdentifier datasetIdentifier = getDatasetIdentifierFromTable(table);
    assertThat(datasetIdentifier.getName()).isEqualTo("mydb.mytable");
    assertThat(datasetIdentifier.getNamespace()).isEqualTo("mycatalog");
    assertThat(datasetIdentifier.getSymlinks()).isEmpty();
  }

  @Test
  void testDatasetIdentifierFromBigQueryTable() {
    org.apache.hadoop.hive.metastore.api.Table metastoreApiTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    metastoreApiTable.setTableName("mytable");
    metastoreApiTable.setDbName("mydb");
    metastoreApiTable.setCatName("mycatalog");
    metastoreApiTable.putToParameters("bq.table", "myproject.mydataset.mytable");
    Table table = new Table(metastoreApiTable);
    DatasetIdentifier datasetIdentifier = getDatasetIdentifierFromTable(table);
    assertThat(datasetIdentifier.getName()).isEqualTo("myproject.mydataset.mytable");
    assertThat(datasetIdentifier.getNamespace()).isEqualTo("bigquery");
    assertThat(datasetIdentifier.getSymlinks()).isEmpty();
  }

  @Test
  void testDatasetIdentifierTableWithLocation() {
    org.apache.hadoop.hive.metastore.api.Table metastoreApiTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    metastoreApiTable.setTableName("mytable");
    metastoreApiTable.setDbName("mydb");
    metastoreApiTable.setCatName("mycatalog");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/path/to/my/table");
    metastoreApiTable.setSd(sd);
    Table table = new Table(metastoreApiTable);
    DatasetIdentifier datasetIdentifier = getDatasetIdentifierFromTable(table);
    assertThat(datasetIdentifier.getName()).isEqualTo("mydb.mytable");
    assertThat(datasetIdentifier.getNamespace()).isEqualTo("mycatalog");
    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    DatasetIdentifier.Symlink symlink = datasetIdentifier.getSymlinks().get(0);
    assertThat(symlink.getName()).isEqualTo("/path/to/my/table");
    assertThat(symlink.getNamespace()).isEqualTo("file");
    assertThat(symlink.getType().name()).isEqualTo("LOCATION");
  }
}
