package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.runtime.AbstractPartialFunction;

public interface VisitorFactory {

  String SPARK2_FACTORY_NAME = "io.openlineage.spark2.agent.lifecycle.plan.VisitorFactoryImpl";
  String SPARK3_FACTORY_NAME = "io.openlineage.spark3.agent.lifecycle.plan.VisitorFactoryImpl";

  default VisitorFactory getInstance(SparkSession session) {
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

  List<AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getInputVisitors();

  List<AbstractPartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> getOutputVisitors();
}
