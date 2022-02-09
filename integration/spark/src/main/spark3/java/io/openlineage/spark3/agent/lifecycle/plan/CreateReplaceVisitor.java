package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.CREATE;
import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.OVERWRITE;

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
import org.apache.spark.sql.catalyst.plans.logical.CreateV2Table;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateReplaceVisitor extends QueryPlanVisitor<LogicalPlan, OpenLineage.OutputDataset> {

  public CreateReplaceVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return (x instanceof CreateTableAsSelect)
        || (x instanceof ReplaceTable)
        || (x instanceof ReplaceTableAsSelect)
        || (x instanceof CreateV2Table);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    TableCatalog tableCatalog;
    Map<String, String> tableProperties;
    Map<String, OpenLineage.DatasetFacet> facetMap = new HashMap<>();
    Identifier identifier;
    StructType schema;
    TableStateChangeFacet.StateChange stateChange;

    if (x instanceof CreateTableAsSelect) {
      CreateTableAsSelect command = (CreateTableAsSelect) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      stateChange = CREATE;
    } else if (x instanceof CreateV2Table) {
      CreateV2Table command = (CreateV2Table) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      stateChange = CREATE;
    } else if (x instanceof ReplaceTable) {
      ReplaceTable command = (ReplaceTable) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      stateChange = OVERWRITE;
    } else {
      ReplaceTableAsSelect command = (ReplaceTableAsSelect) x;
      tableCatalog = command.catalog();
      tableProperties = ScalaConversionUtils.<String, String>fromMap(command.properties());
      identifier = command.tableName();
      schema = command.tableSchema();
      stateChange = OVERWRITE;
    }

    facetMap.put("tableStateChange", new TableStateChangeFacet(stateChange));
    PlanUtils3.includeProviderFacet(tableCatalog, tableProperties, facetMap);

    Optional<DatasetIdentifier> di =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (di.isPresent()) {
      return Collections.singletonList(outputDataset().getDataset(di.get(), schema, facetMap));
    } else {
      return Collections.emptyList();
    }
  }
}
