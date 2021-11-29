package io.openlineage.spark3.agent.lifecycle.plan.catalog;

public class UnsupportedCatalogException extends RuntimeException {
  public UnsupportedCatalogException(String catalog) {
    super(catalog);
  }
}
