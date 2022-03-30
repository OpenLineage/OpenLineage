/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Provides Visitors for iterating on {@link LogicalPlan}.
 *
 * <p>All common Visitors would be grouped and passed to {@link
 * VisitorFactory#getOutputVisitors(SQLContext, String)} to retrieve Visitors for {@link
 * OpenLineage.OutputDataset}
 */
interface VisitorFactory {

  static boolean classPresent(String className) {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(className);
      return true;
    } catch (Exception e) {
      // swallow
    }
    return false;
  }

  List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> getInputVisitors(
      OpenLineageContext context);

  List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
      OpenLineageContext context);
}
