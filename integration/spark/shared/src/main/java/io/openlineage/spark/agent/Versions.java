/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class Versions {

  public static final URI OPEN_LINEAGE_PRODUCER_URI = getProducerUri();

  private static URI getProducerUri() {
    return URI.create(
        String.format(
            "https://github.com/OpenLineage/OpenLineage/tree/%s/integration/spark", getVersion()));
  }

  @SuppressWarnings("PMD")
  private static String getVersion() {
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
