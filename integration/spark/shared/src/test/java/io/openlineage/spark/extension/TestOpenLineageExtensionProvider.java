package io.openlineage.spark.extension;

import io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider;

public class TestOpenLineageExtensionProvider implements OpenLineageExtensionProvider {
  @Override
  public String shadedPackage() {
    return "io.openlineage.shaded";
  }
}
