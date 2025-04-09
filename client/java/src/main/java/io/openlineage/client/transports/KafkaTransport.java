/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public final class KafkaTransport extends Transport {
  private final String topicName;
  private final String messageKey;
  private final KafkaProducer<String, String> producer;

  public KafkaTransport(@NonNull final KafkaConfig kafkaConfig) {
    this(new KafkaProducer<>(kafkaConfig.getProperties()), kafkaConfig);
  }

  public KafkaTransport(
      @NonNull final KafkaProducer<String, String> kafkaProducer,
      @NonNull final KafkaConfig kafkaConfig) {
    this.topicName = kafkaConfig.getTopicName();
    this.messageKey = kafkaConfig.getMessageKey();
    this.producer = kafkaProducer;
  }

  private String getMessageKey(@NonNull OpenLineage.RunEvent runEvent) {
    final OpenLineage.Run run = runEvent.getRun();
    final OpenLineage.Job job = runEvent.getJob();
    if (run == null || job == null) {
      return null;
    }

    /*
      To keep order of events in Kafka topic, we need to send them to the same partition.
      This is the case for:
        1. different runs of the same job.
        2. runs in the chain parent -> child -> grandchild.

      For (1) Kafka messageKey has format "run:<namespace>/<name>".
      For (2) source for `<namespace>` and `<name>` is selected using this order:
      - run.facets.parent.root.job
      - run.facets.parent.job
      - run.job
    */
    String defaultResult = "run:" + job.getNamespace() + "/" + job.getName();

    final OpenLineage.RunFacets runFacets = run.getFacets();
    if (runFacets == null) {
      return defaultResult;
    }

    final OpenLineage.ParentRunFacet parentFacet = runFacets.getParent();
    if (parentFacet == null) {
      return defaultResult;
    }

    final OpenLineage.ParentRunFacetRoot rootParent = parentFacet.getRoot();
    if (rootParent != null) {
      final OpenLineage.RootJob rootJob = rootParent.getJob();
      if (rootJob != null) {
        return "run:" + rootJob.getNamespace() + "/" + rootJob.getName();
      }
    }

    final OpenLineage.ParentRunFacetJob parentJob = parentFacet.getJob();
    if (parentJob == null) {
      return defaultResult;
    }
    return "run:" + parentJob.getNamespace() + "/" + parentJob.getName();
  }

  private String getMessageKey(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    final OpenLineage.Dataset dataset = datasetEvent.getDataset();
    if (dataset == null) {
      return null;
    }

    return "dataset:" + dataset.getNamespace() + "/" + dataset.getName();
  }

  private String getMessageKey(@NonNull OpenLineage.JobEvent jobEvent) {
    final OpenLineage.Job job = jobEvent.getJob();
    if (job == null) {
      return null;
    }

    return "job:" + job.getNamespace() + "/" + job.getName();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(OpenLineageClientUtils.toJson(runEvent), getMessageKey(runEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit(OpenLineageClientUtils.toJson(datasetEvent), getMessageKey(datasetEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emit(OpenLineageClientUtils.toJson(jobEvent), getMessageKey(jobEvent));
  }

  private void emit(String eventAsJson, String eventKey) {
    String partitionKey = messageKey;
    if (partitionKey == null) {
      partitionKey = eventKey;
    }

    final ProducerRecord<String, String> record =
        new ProducerRecord<>(topicName, partitionKey, eventAsJson);
    try {
      producer.send(record);
    } catch (Exception e) {
      log.error("Failed to collect lineage event: {}", eventAsJson, e);
    }
  }

  @Override
  public void close() throws Exception {
    producer.close();
  }
}
