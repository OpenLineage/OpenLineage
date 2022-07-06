/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

public class UnsupportedCatalogException extends RuntimeException {

  public UnsupportedCatalogException(String catalog) {
    super(catalog);
  }
}
