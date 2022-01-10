package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.CREATE;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateTableAsSelectVisitor
    extends QueryPlanVisitor<CreateTableAsSelect, OpenLineage.OutputDataset> {

  public CreateTableAsSelectVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    CreateTableAsSelect command = (CreateTableAsSelect) x;

    Map<String, OpenLineage.DefaultDatasetFacet> facetMap = new HashMap<>();
    Map<String, String> tableProperties =
        ScalaConversionUtils.<String, String>fromMap(command.properties());
    facetMap.put("tableStateChange", new TableStateChangeFacet(CREATE));
    PlanUtils3.includeProviderFacet(command.catalog(), tableProperties, facetMap);

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(
            context, command.catalog(), command.tableName(), tableProperties);

    if (di.isPresent()) {
      return Collections.singletonList(
          outputDataset().getDataset(di.get(), command.tableSchema(), facetMap));
    } else {
      return Collections.emptyList();
    }
  }
}
