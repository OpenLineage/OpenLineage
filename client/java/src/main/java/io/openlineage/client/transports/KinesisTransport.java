package io.openlineage.client.transports;

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
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KinesisTransport extends Transport {
  private final String streamName;
  private final String region;
  private final Optional<String> roleArn;

  private final KinesisProducer producer;

  private final Executor listeningExecutor;

  public KinesisTransport(
      @NonNull final KinesisProducer kinesisProducer, @NonNull final KinesisConfig kinesisConfig) {

    super(Type.KINESIS);
    this.streamName = kinesisConfig.getStreamName();
    this.region = kinesisConfig.getRegion();
    this.roleArn = kinesisConfig.getRoleArn();
    this.producer = kinesisProducer;
    this.listeningExecutor = Executors.newSingleThreadExecutor();
  }

  public KinesisTransport(@NonNull final KinesisConfig kinesisConfig) {
    super(Type.KINESIS);
    this.streamName = kinesisConfig.getStreamName();
    this.region = kinesisConfig.getRegion();
    this.roleArn = kinesisConfig.getRoleArn();

    KinesisProducerConfiguration config =
        KinesisProducerConfiguration.fromProperties(kinesisConfig.getProperties());
    config.setRegion(this.region);
    roleArn.ifPresent(s -> config.setCredentialsProvider(
            new STSAssumeRoleSessionCredentialsProvider.Builder(s, "OLProducer").build()));

    this.producer = new KinesisProducer(config);
    this.listeningExecutor = Executors.newSingleThreadExecutor();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    final String eventAsJson = OpenLineageClientUtils.toJson(runEvent);
    log.debug("Received lineage event: {}", eventAsJson);
    ListenableFuture<UserRecordResult> future =
        this.producer.addUserRecord(
            new UserRecord(
                streamName,
                runEvent.getJob().getNamespace() + ":" + runEvent.getJob().getName(),
                ByteBuffer.wrap(eventAsJson.getBytes())));

    FutureCallback<UserRecordResult> callback =
        new FutureCallback<UserRecordResult>() {
          @Override
          public void onSuccess(UserRecordResult result) {
            log.debug("Success to send to Kinesis lineage event: {}", eventAsJson);
          }

          @Override
          public void onFailure(Throwable t) {
            log.error("Failed to send to Kinesis lineage event: {}", eventAsJson, t);
          };
        };

    Futures.addCallback(future, callback, this.listeningExecutor);
  }
}
