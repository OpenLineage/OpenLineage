package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.util.DatasetIdentifier;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

@Slf4j
public class CosmosHandler implements RelationHandler {

  @Override
  public boolean hasClasses() {
    try {
      CosmosHandler.class.getClassLoader().loadClass("com.azure.cosmos.spark.CosmosCatalog");
      return true;
    } catch (Exception e) {
      log.info("Exception raised in CosmosHandler hasClasses {}", e);
      // swallow- we don't care
    }
    return true; // TODO: figure out how to get the cosmos class - classpath issue that Kusto also
    // has.
  }

  @Override
  public boolean isClass(DataSourceV2Relation relation) {
    return relation.table().name().contains("com.azure.cosmos.spark.items.");
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation) {

    String name;
    String namespace;

    String relationName = relation.table().name().replace("com.azure.cosmos.spark.items.", "");
    int expectedParts = 3;
    String[] tableParts = relationName.split("\\.", expectedParts);

    if (tableParts.length != expectedParts) {
      name = relationName;
      namespace = relationName;
    } else {
      namespace =
          String.format(
              "azurecosmos://%s.documents.azure.com/dbs/%s", tableParts[0], tableParts[1]);
      name = String.format("/colls/%s", tableParts[2]);
    }
    return new DatasetIdentifier(name, namespace);
  }

  @Override
  public String getName() {
    return "cosmos";
  }
}
