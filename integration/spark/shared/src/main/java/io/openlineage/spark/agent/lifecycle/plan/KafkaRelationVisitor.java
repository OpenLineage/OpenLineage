/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.kafka010.KafkaRelation;
import org.apache.spark.sql.kafka010.KafkaSourceProvider;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

/**
 * {@link LogicalPlan} visitor that attempts to extract a {@link OpenLineage.Dataset} from a {@link
 * LogicalRelation}. {@link KafkaRelation} is used to extract topic and bootstrap servers for Kafka
 */
@Slf4j
public class KafkaRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {
  private final DatasetFactory<D> datasetFactory;

  public KafkaRelationVisitor(OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    super(context);
    this.datasetFactory = datasetFactory;
  }

  public static boolean hasKafkaClasses() {
    try {
      KafkaRelationVisitor.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.kafka010.KafkaSourceProvider");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static boolean isKafkaSource(CreatableRelationProvider provider) {
    if (!hasKafkaClasses()) {
      return false;
    }
    return provider instanceof KafkaSourceProvider;
  }

  public static <D extends OpenLineage.Dataset> List<D> createKafkaDatasets(
      DatasetFactory<D> datasetFactory,
      CreatableRelationProvider relationProvider,
      Map<String, String> options,
      SaveMode mode,
      StructType schema) {
    return createDatasetsFromOptions(datasetFactory, options, schema);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof LogicalRelation
        && ((LogicalRelation) x).relation() instanceof KafkaRelation;
  }

  @Override
  @SuppressWarnings("PMD") // we need to make sourceOptionsField accessible
  public List<D> apply(LogicalPlan x) {
    KafkaRelation relation = (KafkaRelation) ((LogicalRelation) x).relation();
    Map<String, String> sourceOptions;
    try {
      Field sourceOptionsField = relation.getClass().getDeclaredField("sourceOptions");
      sourceOptionsField.setAccessible(true);
      sourceOptions = (Map<String, String>) sourceOptionsField.get(relation);
    } catch (Exception e) {
      log.error("Can't extract kafka server options", e);
      sourceOptions = Map$.MODULE$.empty();
    }
    return createDatasetsFromOptions(datasetFactory, sourceOptions, relation.schema());
  }

  private static <D extends OpenLineage.Dataset> List<D> createDatasetsFromOptions(
      DatasetFactory<D> datasetFactory, Map<String, String> sourceOptions, StructType schema) {
    List<String> topics;
    Optional<String> servers = asJavaOptional(sourceOptions.get("kafka.bootstrap.servers"));

    // don't support subscribePattern, as it will report dataset nodes that don't exist

    topics =
        Stream.concat(
                // handle "subscribe" and "topic" here to handle single topic reads/writes
                Stream.of("subscribe", "topic")
                    .map(it -> sourceOptions.get(it))
                    .filter(it -> it.nonEmpty())
                    .map(it -> it.get())
                    .map(String.class::cast),

                // handle "assign" configuration, which specifies topics and the partitions this
                // client will read from
                // "assign" is a json string, with the topics as keys and the values are arrays of
                // the partitions to read
                // E.g., {"topicA":[0,1],"topicB":[2,4]}
                // see
                // https://spark.apache.org/docs/3.1.2/structured-streaming-kafka-integration.html
                ScalaConversionUtils.asJavaOptional(sourceOptions.get("assign"))
                    .map(
                        (String str) -> {
                          try {
                            JsonNode jsonNode = new ObjectMapper().readTree(str);
                            long fieldCount = jsonNode.size();
                            return StreamSupport.stream(
                                Spliterators.spliterator(
                                    jsonNode.fieldNames(),
                                    fieldCount,
                                    Spliterator.SIZED & Spliterator.IMMUTABLE),
                                false);
                          } catch (IOException e) {
                            log.warn(
                                "Unable to find topics from Kafka source configuration {}", str, e);
                          }
                          return Stream.<String>empty();
                        })
                    .orElse(Stream.empty()))
            .collect(Collectors.toList());

    String server =
        servers
            .map(
                str -> {
                  if (!str.matches("\\w+://.*")) {
                    return "PLAINTEXT://" + str;
                  } else {
                    return str;
                  }
                })
            .map(str -> URI.create(str.split(",")[0]))
            .map(uri -> uri.getHost() + ":" + uri.getPort())
            .orElse("");
    String namespace = "kafka://" + server;
    return topics.stream()
        .map(topic -> datasetFactory.getDataset(topic, namespace, schema))
        .collect(Collectors.toList());
  }
}
