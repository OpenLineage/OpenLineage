package marquez.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import marquez.spark.agent.client.DatasetParser;
import marquez.spark.agent.client.DatasetParser.DatasetParseResult;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.client.LineageEvent.DatasetFacet;
import marquez.spark.agent.client.LineageEvent.SchemaDatasetFacet;
import marquez.spark.agent.client.LineageEvent.SchemaField;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DatasetLogicalPlanTraverser extends LogicalPlanTraverser {
  private Set<Dataset> outputDatasets;
  private Set<Dataset> inputDatasets;
  private String jobNamespace;

  public TraverserResult build(LogicalPlan plan, String jobNamespace) {
    synchronized (this) {
      outputDatasets = new HashSet<>();
      inputDatasets = new HashSet<>();
      this.jobNamespace = jobNamespace;
      super.visit(plan);
      return new TraverserResult(new ArrayList<>(outputDatasets), new ArrayList<>(inputDatasets));
    }
  }

  @Override
  protected Object visit(
      CreateDataSourceTableAsSelectCommand createDataSourceTableAsSelectCommand) {
    outputDatasets.add(
        buildDataset(
            createDataSourceTableAsSelectCommand.table().qualifiedName(),
            DatasetFacet.builder()
                .schema(visit(createDataSourceTableAsSelectCommand.table().schema()))
                .build()));
    return null;
  }

  protected Object visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRelationCommand) {
    outputDatasets.add(
        buildDataset(visitPathUri(insertIntoHadoopFsRelationCommand.outputPath().toUri()), null));
    return null;
  }

  protected Dataset buildDataset(String uri, DatasetFacet datasetFacet) {
    DatasetParseResult result = DatasetParser.parse(uri);
    return buildDataset(result, datasetFacet);
  }

  protected Dataset buildDataset(URI uri, DatasetFacet datasetFacet) {
    DatasetParseResult result = DatasetParser.parse(uri);
    return buildDataset(result, datasetFacet);
  }

  protected Dataset buildDataset(DatasetParseResult result, DatasetFacet datasetFacet) {
    return Dataset.builder()
        .name(result.getName())
        .namespace(result.getNamespace())
        .facets(datasetFacet)
        .build();
  }

  protected SchemaDatasetFacet visit(StructType structType) {
    return SchemaDatasetFacet.builder()
        ._producer(URI.create("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"))
        ._schemaURL(
            URI.create("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/schemaDatasetFacet"))
        .fields(visit(structType.fields()))
        .build();
  }

  protected List<SchemaField> visit(StructField[] fields) {
    List<SchemaField> list = new ArrayList<>();
    for (StructField field : fields) {
      list.add(visit(field));
    }
    return list;
  }

  protected SchemaField visit(StructField field) {
    return SchemaField.builder().name(field.name()).type(field.dataType().typeName()).build();
  }

  protected Object visit(HadoopFsRelation relation) {
    inputDatasets.addAll(visit(relation.location()));
    return null;
  }

  protected List<Dataset> visit(FileIndex fileIndex) {
    return visitPaths(asJavaCollection(fileIndex.rootPaths()));
  }

  protected List<Dataset> visitPaths(Collection<Path> paths) {
    List<Dataset> list = new ArrayList<>();
    for (Path path : paths) {
      list.add(visit(path));
    }
    return list;
  }

  protected Dataset visit(Path path) {
    return buildDataset(visitPathUri(path.toUri()), null);
  }

  protected URI visitPathUri(URI uri) {
    return uri;
  }

  @Getter
  public class TraverserResult {

    private final List<Dataset> outputDataset;
    private final List<Dataset> inputDataset;

    public TraverserResult(List<Dataset> outputDataset, List<Dataset> inputDataset) {
      this.outputDataset = outputDataset;
      this.inputDataset = inputDataset;
    }
  }
}
