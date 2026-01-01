/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class Versions {

  public static final URI OPEN_LINEAGE_PRODUCER_URI = getProducerUri();

  // TODO: Figure out the proper producer URL. Should we use the official OpenLineage (assuming
  //  that they'll eventually adopt it, or should we use our own custom repo URL?
  //  See b/369430651
  private static URI getProducerUri() {
    return URI.create(
        String.format(
            "https://github.com/OpenLineage/OpenLineage/tree/%s/integration/hive", getVersion()));
  }

  @SuppressWarnings("PMD")
  public static String getVersion() {
    try {
      Properties properties = new Properties();
      InputStream is = Versions.class.getResourceAsStream("version.properties");
      properties.load(is);
      return properties.getProperty("version");
    } catch (IOException exception) {
      return "main";
    }
  }
}
