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

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import java.net.URI;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HiveUtils {

  public static Table getTable(Configuration conf, String dbName, String tableName) {
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    try {
      return Hive.get(hiveConf).getTable(dbName, tableName);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  public static DatasetIdentifier getDatasetIdentifierFromTable(Table table) {
    if (table.getParameters() != null && table.getParameters().get("bq.table") != null) {
      return new DatasetIdentifier(table.getParameters().get("bq.table"), "bigquery");
    }
    if (table.getSd() != null && table.getSd().getLocation() != null) {
      URI uri = new URI(table.getSd().getLocation());
      DatasetIdentifier di = FilesystemDatasetUtils.fromLocation(uri);
      return di.withSymlink(
          table.getFullyQualifiedName(), table.getCatName(), DatasetIdentifier.SymlinkType.TABLE);
    }
    return new DatasetIdentifier(table.getFullyQualifiedName(), table.getCatName());
  }
}
