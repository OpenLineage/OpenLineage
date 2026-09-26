/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import io.openlineage.spark.agent.DynamicParameter;
import lombok.Getter;

/**
 * Dynamic parameters of the Azure Unity Catalog integration tests. Each parameter can be set as a
 * system property ({@code -Dopenlineage.tests.azure.<parameterName>=value}) or as the corresponding
 * environment variable (e.g. {@code OPENLINEAGE_TESTS_AZURE_CONTAINER_URI})
 */
@Getter
public enum AzureDynamicParameter implements DynamicParameter {

  /**
   * Writable {@code abfss://...} base location under which the tests provision tables, e.g. {@code
   * abfss://container@account.dfs.core.windows.net/test}. Required.
   */
  ContainerUri("containerUri"),

  /** OAuth service-principal client id. */
  ClientId("clientId"),

  /** OAuth service-principal client secret. */
  ClientSecret("clientSecret"),

  /** OAuth service-principal tenant id (a GUID). */
  TenantId("tenantId");

  private final String parameterName;
  private final String prefix;

  AzureDynamicParameter(String parameterName) {
    this.parameterName = parameterName;
    this.prefix = "openlineage.tests.azure";
  }

  @Override
  public String getDefaultValue() {
    return null;
  }
}
