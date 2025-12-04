/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import java.util.Arrays;

public class GravitinoUtils {

  private static final String DEFAULT_CATALOG_NAME = "spark_catalog";

  // For datasource v1 jdbc relation
  public static DatasetIdentifier getGravitinoDatasetIdentifierFromJdbc(String jdbcName) {
    GravitinoInfoProviderImpl provider = GravitinoInfoProviderImpl.getInstance();
    String metalake = provider.getMetalakeName();
    String catalogName = provider.getGravitinoCatalog(DEFAULT_CATALOG_NAME);
    String identifierName = String.join(".", Arrays.asList(catalogName, jdbcName));
    return new DatasetIdentifier(identifierName, metalake);
  }
}
