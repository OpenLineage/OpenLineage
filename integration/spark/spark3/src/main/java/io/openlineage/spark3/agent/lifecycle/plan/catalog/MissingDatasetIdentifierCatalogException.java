/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

/**
 * Exception thrown when a dataset identifier cannot be extracted for a given catalog. This can
 * happen for tables which are going to be created.
 */
public class MissingDatasetIdentifierCatalogException extends RuntimeException {

  public MissingDatasetIdentifierCatalogException(String catalog) {
    super(catalog);
  }
}
