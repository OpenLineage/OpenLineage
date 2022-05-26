/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import org.apache.spark.package$;

class VisitorFactoryProvider {

  private static final String SPARK2_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark2VisitorFactoryImpl";
  private static final String SPARK3_FACTORY_NAME =
      "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl";

  static VisitorFactory getInstance() {
    return getInstance(package$.MODULE$.SPARK_VERSION());
  }

  static VisitorFactory getInstance(String version) {
    try {
      if (version.startsWith("2.")) {
        return (VisitorFactory) Class.forName(SPARK2_FACTORY_NAME).newInstance();
      } else {
        return (VisitorFactory) Class.forName(SPARK3_FACTORY_NAME).newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Can't instantiate visitor factory for version: %s", version), e);
    }
  }
}
