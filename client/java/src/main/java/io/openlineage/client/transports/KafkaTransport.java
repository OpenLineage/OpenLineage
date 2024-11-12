/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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

    final OpenLineage.RunFacets runFacets = run.getFacets();
    if (runFacets != null) {
      final OpenLineage.ParentRunFacet parentRunFacet = runFacets.getParent();
      if (parentRunFacet != null) {
        final OpenLineage.ParentRunFacetJob parentJob = parentRunFacet.getJob();
        final OpenLineage.ParentRunFacetRun parentRun = parentRunFacet.getRun();
        if (parentRun != null && parentJob != null) {
          return "run:" + parentJob.getNamespace() + "/" + parentJob.getName();
        }
      }
    }
    return "run:" + job.getNamespace() + "/" + job.getName();
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
