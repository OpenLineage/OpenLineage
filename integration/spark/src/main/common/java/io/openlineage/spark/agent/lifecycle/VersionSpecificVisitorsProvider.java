package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.lifecycle.plan.VisitorFactory;
import org.apache.spark.sql.SparkSession;

public class VersionSpecificVisitorsProvider {

  private static final String SPARK2_FACTORY_NAME =
      "io.openlineage.spark2.agent.lifecycle.plan.VisitorFactoryImpl";
  private static final String SPARK3_FACTORY_NAME =
      "io.openlineage.spark3.agent.lifecycle.plan.VisitorFactoryImpl";

  static VisitorFactory getInstance(SparkSession session) {
    try {
      if (session.version().startsWith("2.")) {
        return (VisitorFactory) Class.forName(SPARK2_FACTORY_NAME).newInstance();
      } else {
        return (VisitorFactory) Class.forName(SPARK3_FACTORY_NAME).newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Can't instantiate visitor factory for version: %s", session.version()), e);
    }
  }
}
