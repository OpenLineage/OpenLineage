package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.AddFileCommand;
import org.apache.spark.sql.execution.command.AddJarCommand;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestQueryPlanVisitor {

  @Test
  public void testIsDefinedAt() {
    // construct an anonymous class with an AddJarCommand type parameter
    QueryPlanVisitor<AddJarCommand, OpenLineage.Dataset> visitor =
        new QueryPlanVisitor<AddJarCommand, OpenLineage.Dataset>() {
          @Override
          public List<OpenLineage.Dataset> apply(LogicalPlan x) {
            return Collections.emptyList();
          }
        };
    Predicate<LogicalPlan> logicalPlanPredicate = visitor::isDefinedAt;
    Assertions.assertThat(logicalPlanPredicate)
        .accepts(new AddJarCommand("/a/b/c.jar"))
        .rejects(new AddFileCommand("/a/b/c.txt"));
  }

  @Test
  public void testIsDefinedAtNoGenericParam() {
    // construct an anonymous class with an AddJarCommand type parameter
    QueryPlanVisitor visitor =
        new QueryPlanVisitor() {
          public List<OpenLineage.Dataset> apply(LogicalPlan x) {
            return Collections.emptyList();
          }
        };
    Predicate<LogicalPlan> logicalPlanPredicate = visitor::isDefinedAt;
    Assertions.assertThat(logicalPlanPredicate)
        .rejects(new AddJarCommand("/a/b/c.jar"))
        .rejects(new AddFileCommand("/a/b/c.txt"));
  }
}
