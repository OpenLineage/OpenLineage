/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.kinesis;

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class KinesisTransport extends Transport {
  private final String streamName;
  private final String region;
  private final String roleArn;

  private final KinesisProducer producer;

  private final Executor listeningExecutor;

  public KinesisTransport(
      @NonNull final KinesisProducer kinesisProducer, @NonNull final KinesisConfig kinesisConfig) {
    this.streamName = kinesisConfig.getStreamName();
    this.region = kinesisConfig.getRegion();
    this.roleArn = kinesisConfig.getRoleArn();
    this.producer = kinesisProducer;
    this.listeningExecutor = Executors.newSingleThreadExecutor();
  }

  public KinesisTransport(@NonNull final KinesisConfig kinesisConfig) {
    this.streamName = kinesisConfig.getStreamName();
    this.region = kinesisConfig.getRegion();
    this.roleArn = kinesisConfig.getRoleArn();
    KinesisProducerConfiguration config =
        KinesisProducerConfiguration.fromProperties(kinesisConfig.getProperties());
    config.setRegion(this.region);
    if (StringUtils.isNotBlank(roleArn)) {
      config.setCredentialsProvider(
          new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "OLProducer").build());
    }
    this.producer = new KinesisProducer(config);
    this.listeningExecutor = Executors.newSingleThreadExecutor();
  }

  private String getPartitionKey(@NonNull OpenLineage.RunEvent runEvent) {
    /*
      To keep order of events in Kinesis stream, we need to send them to the same partition.
      This is the case for:
        1. different runs of the same job.
        2. runs in the chain parent -> child -> grandchild.

      For (1) Kinesis partitionKey has format "run:<namespace>/<name>".
      For (2) source for `<namespace>` and `<name>` is selected using this order:
      - run.facets.parent.root.job
      - run.facets.parent.job
      - run.job
    */
    final OpenLineage.Run run = runEvent.getRun();
    final OpenLineage.Job job = runEvent.getJob();
    final String defaultResult = "run:" + job.getNamespace() + "/" + job.getName();

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

  private String getPartitionKey(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    final OpenLineage.Dataset dataset = datasetEvent.getDataset();
    return "dataset:" + dataset.getNamespace() + "/" + dataset.getName();
  }

  private String getPartitionKey(@NonNull OpenLineage.JobEvent jobEvent) {
    final OpenLineage.Job job = jobEvent.getJob();
    return "job:" + job.getNamespace() + "/" + job.getName();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(OpenLineageClientUtils.toJson(runEvent), getPartitionKey(runEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit(OpenLineageClientUtils.toJson(datasetEvent), getPartitionKey(datasetEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emit(OpenLineageClientUtils.toJson(jobEvent), getPartitionKey(jobEvent));
  }

  private void emit(String eventAsJson, String partitionKey) {
    ListenableFuture<UserRecordResult> future =
        this.producer.addUserRecord(
            new UserRecord(streamName, partitionKey, ByteBuffer.wrap(eventAsJson.getBytes())));

    FutureCallback<UserRecordResult> callback =
        new FutureCallback<UserRecordResult>() {
          @Override
          public void onSuccess(UserRecordResult result) {
            log.debug("Success to send to Kinesis lineage event: {}", eventAsJson);
          }

          @Override
          public void onFailure(Throwable t) {
            log.error("Failed to send to Kinesis lineage event: {}", eventAsJson, t);
          }
        };

    Futures.addCallback(future, callback, this.listeningExecutor);
  }
}
