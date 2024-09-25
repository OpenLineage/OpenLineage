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
package io.openlineage.hive.util;

import static io.openlineage.hive.util.HiveUtils.getDatasetIdentifierFromTable;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.jupiter.api.Test;

public class HiveUtilsTest {

  @Test
  public void testDatasetIdentifierFromSimpleTable() {
    org.apache.hadoop.hive.metastore.api.Table tTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    tTable.setTableName("mytable");
    tTable.setDbName("mydb");
    tTable.setCatName("mycatalog");
    Table table = new Table(tTable);
    DatasetIdentifier datasetIdentifier = getDatasetIdentifierFromTable(table);
    assertThat(datasetIdentifier.getName()).isEqualTo("mydb.mytable");
    assertThat(datasetIdentifier.getNamespace()).isEqualTo("mycatalog");
    assertThat(datasetIdentifier.getSymlinks()).isEmpty();
  }

  @Test
  public void testDatasetIdentifierFromBigQueryTable() {
    org.apache.hadoop.hive.metastore.api.Table tTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    tTable.setTableName("mytable");
    tTable.setDbName("mydb");
    tTable.setCatName("mycatalog");
    tTable.putToParameters("bq.table", "myproject.mydataset.mytable");
    Table table = new Table(tTable);
    DatasetIdentifier datasetIdentifier = getDatasetIdentifierFromTable(table);
    assertThat(datasetIdentifier.getName()).isEqualTo("myproject.mydataset.mytable");
    assertThat(datasetIdentifier.getNamespace()).isEqualTo("bigquery");
    assertThat(datasetIdentifier.getSymlinks()).isEmpty();
  }

  @Test
  public void testDatasetIdentifierTableWithLocation() {
    org.apache.hadoop.hive.metastore.api.Table tTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    tTable.setTableName("mytable");
    tTable.setDbName("mydb");
    tTable.setCatName("mycatalog");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/path/to/my/table");
    tTable.setSd(sd);
    Table table = new Table(tTable);
    DatasetIdentifier datasetIdentifier = getDatasetIdentifierFromTable(table);
    assertThat(datasetIdentifier.getName()).isEqualTo("/path/to/my/table");
    assertThat(datasetIdentifier.getNamespace()).isEqualTo("file");
    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    DatasetIdentifier.Symlink symlink = datasetIdentifier.getSymlinks().get(0);
    assertThat(symlink.getName()).isEqualTo("mydb.mytable");
    assertThat(symlink.getNamespace()).isEqualTo("mycatalog");
    assertThat(symlink.getType().name()).isEqualTo("TABLE");
  }
}
