/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    final String eventAsJson = OpenLineageClientUtils.toJson(runEvent);
    final OpenLineage.Job job = runEvent.getJob();
    final String partitionKey = "run:" + job.getNamespace() + "/" + job.getName();
    emit(eventAsJson, partitionKey);
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    final String eventAsJson = OpenLineageClientUtils.toJson(datasetEvent);
    final OpenLineage.Dataset dataset = datasetEvent.getDataset();
    final String partitionKey = "dataset:" + dataset.getNamespace() + "/" + dataset.getName();
    emit(eventAsJson, partitionKey);
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    final String eventAsJson = OpenLineageClientUtils.toJson(jobEvent);
    final OpenLineage.Job job = jobEvent.getJob();
    final String partitionKey = "job:" + job.getNamespace() + "/" + job.getName();
    emit(eventAsJson, partitionKey);
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
