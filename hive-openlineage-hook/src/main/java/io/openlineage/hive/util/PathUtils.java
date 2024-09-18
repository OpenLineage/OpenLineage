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
import org.apache.hadoop.hive.ql.metadata.Table;

public class PathUtils {

  @SneakyThrows
  public static DatasetIdentifier fromTable(Table table) {
    String bqTable = table.getParameters().get("bq.table");
    if (bqTable != null) {
      return new DatasetIdentifier(bqTable, "bigquery");
    }
    String location = table.getSd().getLocation();
    if (location != null) {
      URI uri = new URI(location);
      DatasetIdentifier di = FilesystemDatasetUtils.fromLocation(uri);
      return di.withSymlink(
          table.getFullyQualifiedName(), table.getCatName(), DatasetIdentifier.SymlinkType.TABLE);
    }
    return new DatasetIdentifier(table.getFullyQualifiedName(), table.getCatName());
  }
}
