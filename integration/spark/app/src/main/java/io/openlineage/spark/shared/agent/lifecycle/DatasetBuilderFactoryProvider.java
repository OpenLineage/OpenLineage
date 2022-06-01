package io.openlineage.spark.shared.agent.lifecycle;

import io.openlineage.spark.shared.agent.lifecycle.DatasetBuilderFactory;
import org.apache.spark.package$;

public class DatasetBuilderFactoryProvider {

  private static final String SPARK2_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark2DatasetBuilderFactory";
  private static final String SPARK3_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark3DatasetBuilderFactory";

  static DatasetBuilderFactory getInstance() {
    return getInstance(package$.MODULE$.SPARK_VERSION());
  }

  static DatasetBuilderFactory getInstance(String version) {
    try {
      if (version.startsWith("2.")) {
        return (DatasetBuilderFactory) Class.forName(SPARK2_FACTORY_NAME).newInstance();
      } else {
        return (DatasetBuilderFactory) Class.forName(SPARK3_FACTORY_NAME).newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Can't instantiate dataset builder factory factory for version: %s", version),
          e);
    }
  }
}
