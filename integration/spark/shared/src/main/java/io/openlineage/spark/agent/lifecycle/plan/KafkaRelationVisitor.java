/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.kafka010.KafkaRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.Nullable;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

/**
 * {@link LogicalPlan} visitor that attempts to extract a {@link OpenLineage.Dataset} from a {@link
 * LogicalRelation}. {@link KafkaRelation} is used to extract topic and bootstrap servers for Kafka
 */
@Slf4j
public class KafkaRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {
  private static final String KAFKA_SOURCE_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaSourceProvider";
  private static final String KAFKA_RELATION_CLASS_NAME =
      "org.apache.spark.sql.kafka010.KafkaRelation";
  private static final AtomicBoolean KAFKA_PROVIDER_CLASS_PRESENT = new AtomicBoolean(false);
  private static final AtomicBoolean KAFKA_PROVIDER_CHECKED = new AtomicBoolean(false);
  private static final AtomicBoolean KAFKA_RELATION_CLASS_PRESENT = new AtomicBoolean(false);
  private static final AtomicBoolean KAFKA_RELATION_CHECKED = new AtomicBoolean(false);

  private final DatasetFactory<D> datasetFactory;

  public KafkaRelationVisitor(OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    super(context);
    this.datasetFactory = datasetFactory;
  }

  /**
   * Checking the KafkaSourceProvider class with both
   * KafkaRelationVisitor.class.getClassLoader.loadClass and
   * Thread.currentThread().getContextClassLoader().loadClass. The first checks if the class is
   * present on the classpath, and the second one is a catchall which captures if the class has been
   * installed. When job submission happens via '--packages' the first check fails, and we revert to
   * the second check.
   */
  public static boolean hasKafkaClasses() {
    log.debug("Checking if Kafka classes are available");
    if (!KAFKA_PROVIDER_CHECKED.get()) {
      log.debug("Kafka classes have not been checked yet");
      synchronized (KafkaRelationVisitor.class) {
        if (!KAFKA_PROVIDER_CHECKED.get()) {
          boolean available =
              checkWithCurrentClassClassLoader(KAFKA_SOURCE_PROVIDER_CLASS_NAME)
                  || checkWithCurrentThreadContextClassLoader(KAFKA_SOURCE_PROVIDER_CLASS_NAME);
          KAFKA_PROVIDER_CLASS_PRESENT.set(available);
          KAFKA_PROVIDER_CHECKED.set(true);
          log.debug("Kafka classes availability: " + available);
        }
      }
    }
    return KAFKA_PROVIDER_CLASS_PRESENT.get();
  }

  private static boolean hasKafkaRelationClass() {
    log.debug("Checking if KafkaRelation class is available");
    if (!KAFKA_RELATION_CHECKED.get()) {
      log.debug("KafkaRelation class has not been checked yet");
      synchronized (KafkaRelationVisitor.class) {
        if (!KAFKA_RELATION_CHECKED.get()) {
          boolean available =
              checkWithCurrentClassClassLoader(KAFKA_RELATION_CLASS_NAME)
                  || checkWithCurrentThreadContextClassLoader(KAFKA_RELATION_CLASS_NAME);
          KAFKA_RELATION_CLASS_PRESENT.set(available);
          KAFKA_RELATION_CHECKED.set(true);
          log.debug("KafkaRelation class availability: " + available);
        }
      }
    }
    return KAFKA_RELATION_CLASS_PRESENT.get();
  }

  private static boolean checkWithCurrentClassClassLoader(String className) {
    try {
      KafkaRelationVisitor.class.getClassLoader().loadClass(className);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean checkWithCurrentThreadContextClassLoader(String className) {
    return loadClassWithCurrentThreadContextClassLoader(className) != null;
  }

  private static @Nullable Class<?> loadClassWithCurrentThreadContextClassLoader(String className) {
    try {
      return Thread.currentThread().getContextClassLoader().loadClass(className);
    } catch (Exception e) {
      return null;
    }
  }

  public static boolean isKafkaSource(CreatableRelationProvider provider) {
    log.debug("Checking if provider is KafkaSourceProvider");
    if (!hasKafkaClasses()) {
      log.debug("Kafka classes are not available to check whether provider is KafkaSourceProvider");
      return false;
    }

    try {
      log.debug("Checking if provider is KafkaSourceProvider");
      Class<?> c = loadClassWithCurrentThreadContextClassLoader(KAFKA_SOURCE_PROVIDER_CLASS_NAME);
      if (c == null) {
        return false;
      }
      return c.isAssignableFrom(provider.getClass());
    } catch (Exception e) {
      return false;
    }
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
    if (!hasKafkaRelationClass()) {
      return false;
    }

    if (!(x instanceof LogicalRelation)) {
      return false;
    }

    Class<?> c = loadClassWithCurrentThreadContextClassLoader(KAFKA_RELATION_CLASS_NAME);
    if (c == null) {
      return false;
    }

    LogicalRelation logicalRelation = ((LogicalRelation) x);
    BaseRelation baseRelation = logicalRelation.relation();
    if (log.isDebugEnabled()) {
        log.debug("Checking if {} is assignable from {}", c.getCanonicalName(), baseRelation.getClass().getCanonicalName());
    }
    return c.isAssignableFrom(baseRelation.getClass());
  }

  @Override
  @SuppressWarnings("PMD") // we need to make sourceOptionsField accessible
  public List<D> apply(LogicalPlan x) {
    LogicalRelation logicalRelation = (LogicalRelation) x;
    BaseRelation relation = logicalRelation.relation();
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
