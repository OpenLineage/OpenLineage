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
import io.openlineage.client.utils.DatasetIdentifierUtils;
import java.io.File;
import java.net.URI;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";
  private static final String DEFAULT_SEPARATOR = "/";

  public static String getTableLocation(Table table, Configuration conf) {
    if (table.getSd().getLocation() != null) {
      return table.getSd().getLocation();
    }
    // In some cases (e.g. CTAS) the location value isn't provided to the hook in the
    // output `Table` object. So we run an explicit call to the Hive Metastore to retrieve
    // the value.
    HiveConf hiveConf = new HiveConf(conf, PathUtils.class);
    try {
      Table metaTable = Hive.get(hiveConf).getTable(table.getDbName(), table.getTableName());
      return metaTable.getSd().getLocation();
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  public static DatasetIdentifier fromTable(Table table, Configuration conf) {
    String bqTable = table.getParameters().get("bq.table");
    if (bqTable != null) {
      return new DatasetIdentifier(bqTable, "bigquery");
    }
    String location = getTableLocation(table, conf);
    if (location != null) {
      URI uri = prepareUriFromLocation(location);
      DatasetIdentifier di = DatasetIdentifierUtils.fromURI(uri, DEFAULT_SCHEME);
      return di.withSymlink(
          table.getFullyQualifiedName(),
          StringUtils.substringBeforeLast(uri.toString(), File.separator),
          DatasetIdentifier.SymlinkType.TABLE);
    }
    return new DatasetIdentifier(table.getFullyQualifiedName(), "UNKNOWN"); // FIXME: namespace
  }

  @SneakyThrows
  private static URI prepareUriFromLocation(String location) {
    URI uri = new URI(location);
    if (uri.getPath() != null
        && uri.getPath().startsWith(DEFAULT_SEPARATOR)
        && uri.getScheme() == null) {
      uri = new URI(DEFAULT_SCHEME, null, uri.getPath(), null, null);
    } else if (uri.getScheme() != null && uri.getScheme().equals(DEFAULT_SCHEME)) {
      // Normalize the URI if it is already a file scheme but has three slashes
      String path = uri.getPath();
      if (uri.toString().startsWith(DEFAULT_SCHEME + ":///")) {
        uri = new URI(DEFAULT_SCHEME, null, path, null, null);
      }
    }
    return uri;
  }
}
