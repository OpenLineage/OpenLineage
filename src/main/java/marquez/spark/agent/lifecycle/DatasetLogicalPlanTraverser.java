package marquez.spark.agent.lifecycle;

import static scala.collection.JavaConversions.asJavaCollection;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Value;
import marquez.spark.agent.client.DatasetParser;
import marquez.spark.agent.client.DatasetParser.DatasetParseResult;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.client.LineageEvent.DatasetFacet;
import marquez.spark.agent.client.LineageEvent.SchemaDatasetFacet;
import marquez.spark.agent.client.LineageEvent.SchemaField;
import marquez.spark.agent.facets.OutputStatisticsFacet;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand;
import org.apache.spark.sql.execution.datasources.FileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

public class DatasetLogicalPlanTraverser extends LogicalPlanTraverser {
  private Set<Dataset> outputDatasets;
  private Set<Dataset> inputDatasets;
  private Statistics statistics;
  private String jobNamespace;

  public TraverserResult build(LogicalPlan plan, String jobNamespace) {
    synchronized (this) {
      outputDatasets = new HashSet<>();
      inputDatasets = new HashSet<>();
      this.jobNamespace = jobNamespace;
      super.visit(plan);
      return new TraverserResult(
          new ArrayList<>(outputDatasets), new ArrayList<>(inputDatasets), statistics);
    }
  }

  @Override
  protected Object visit(
      CreateDataSourceTableAsSelectCommand createDataSourceTableAsSelectCommand) {
    OutputStatisticsFacet outputStats =
        getOutputStats(createDataSourceTableAsSelectCommand.metrics());
    apply(
        buildDataset(
            createDataSourceTableAsSelectCommand.table().qualifiedName(),
            DatasetFacet.builder()
                .schema(visit(createDataSourceTableAsSelectCommand.table().schema()))
                .additional(ImmutableMap.of("stats", outputStats))
                .build()));
    return null;
  }

  @Override
  protected Object visit(InsertIntoDataSourceDirCommand insertIntoDataSourceCommand) {
    OutputStatisticsFacet outputStats = getOutputStats(insertIntoDataSourceCommand.metrics());
    DatasetFacet datasetFacet =
        DatasetFacet.builder()
            .schema(visit(insertIntoDataSourceCommand.schema()))
            .additional(ImmutableMap.of("stats", outputStats))
            .build();
    DatasetLogicalPlanTraverser traverser = this;
    insertIntoDataSourceCommand
        .storage()
        .locationUri()
        .map(
            new AbstractFunction1<URI, Dataset>() {
              @Override
              public Dataset apply(URI uri) {
                return buildDataset(visitPathUri(uri), datasetFacet);
              }
            })
        .foreach(
            new AbstractFunction1<Dataset, Void>() {
              @Override
              public Void apply(Dataset v1) {
                traverser.apply(v1);
                return null;
              }
            });
    return super.visit(insertIntoDataSourceCommand);
  }

  private OutputStatisticsFacet getOutputStats(Map<String, SQLMetric> metrics) {
    long rowCount =
        metrics
            .getOrElse(
                "numOutputRows",
                new AbstractFunction0<SQLMetric>() {
                  @Override
                  public SQLMetric apply() {
                    return new SQLMetric("sum", 0L);
                  }
                })
            .value();
    long outputBytes =
        metrics
            .getOrElse(
                "numOutputBytes",
                new AbstractFunction0<SQLMetric>() {
                  @Override
                  public SQLMetric apply() {
                    return new SQLMetric("sum", 0L);
                  }
                })
            .value();
    return new OutputStatisticsFacet(rowCount, outputBytes);
  }

  protected Object visit(InsertIntoHadoopFsRelationCommand insertIntoHadoopFsRelationCommand) {
    OutputStatisticsFacet outputStats = getOutputStats(insertIntoHadoopFsRelationCommand.metrics());
    DatasetFacet datasetFacet =
        DatasetFacet.builder()
            .schema(visit(insertIntoHadoopFsRelationCommand.schema()))
            .additional(ImmutableMap.of("stats", outputStats))
            .build();
    outputDatasets.add(
        buildDataset(
            visitPathUri(insertIntoHadoopFsRelationCommand.outputPath().toUri()), datasetFacet));
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

  @Override
  protected Object visitStatistics(Statistics stats) {
    this.statistics = stats;
    return null;
  }

  private Boolean apply(Dataset uri) {
    return outputDatasets.add(uri);
  }

  @Value
  public static class TraverserResult {

    List<Dataset> outputDataset;
    List<Dataset> inputDataset;
    Statistics statistics;
  }
}
