/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class GravitinoInfoProviderImpl {
  private GravitinoInfo gravitinoInfo;
  private List<GravitinoInfoProvider> providers = Arrays.asList(new SparkGravitinoInfoProvider());

  private static class Holder {
    private static final GravitinoInfoProviderImpl INSTANCE = new GravitinoInfoProviderImpl();
  }

  public static GravitinoInfoProviderImpl getInstance() {
    return Holder.INSTANCE;
  }

  public static GravitinoInfoProviderImpl newInstanceForTest() {
    return new GravitinoInfoProviderImpl();
  }

  private GravitinoInfoProviderImpl() {}

  public boolean useGravitinoIdentifier() {
    return getGravitinoInfo().isUseGravitinoIdentifier();
  }

  public String getGravitinoCatalog(String originCatalogName) {
    return getGravitinoInfo()
        .getCatalogMapping()
        .getOrDefault(originCatalogName, originCatalogName);
  }

  public String getMetalakeName() {
    Optional<String> metalake = getGravitinoInfo().getMetalake();
    if (!metalake.isPresent()) {
      throw new RuntimeException("Couldn't get Gravitino metalake");
    }
    return metalake.get();
  }

  private GravitinoInfo getGravitinoInfo() {
    if (gravitinoInfo != null) return gravitinoInfo;
    synchronized (this) {
      if (gravitinoInfo != null) {
        return gravitinoInfo;
      }
      gravitinoInfo = doGetGravitinoInfo();
    }
    return gravitinoInfo;
  }

  private GravitinoInfo doGetGravitinoInfo() {
    for (GravitinoInfoProvider provider : providers) {
      if (provider.isAvailable()) {
        return provider.getGravitinoInfo();
      }
    }
    throw new IllegalStateException("Could not find Gravitino info");
  }
}
