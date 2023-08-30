/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import static io.openlineage.flink.StreamEnvironment.setupEnv;
import static io.openlineage.kafka.KafkaClientProvider.aKafkaSink;
import static io.openlineage.kafka.KafkaClientProvider.aKafkaSource;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.flink.avro.event.InputEvent;
import io.openlineage.flink.tracker.restapi.Checkpoints;
import io.openlineage.util.OpenLineageFlinkJobListenerBuilder;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkStoppableApplication {
  private static final String TOPIC_PARAM_SEPARATOR = ",";
  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkStoppableApplication.class);

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromArgs(args);
    StreamExecutionEnvironment env = setupEnv(args);

    env.fromSource(aKafkaSource(parameters.getRequired("input-topics").split(TOPIC_PARAM_SEPARATOR)), noWatermarks(), "kafka-source").uid("kafka-source")
            .keyBy(InputEvent::getId)
            .process(new StatefulCounter()).name("process").uid("process")
            .sinkTo(aKafkaSink(parameters.getRequired("output-topic"))).name("kafka-sink").uid("kafka-sink");

    env.registerJobListener(
        OpenLineageFlinkJobListenerBuilder
            .create()
            .executionEnvironment(env)
            .jobName("flink-stoppable-job")
            .jobTrackingInterval(Duration.ofSeconds(1))
            .build()
    );

    JobClient jobClient = env.executeAsync("flink-stoppable-job");

    // wait until job is running
    Awaitility.await().until(() -> jobClient.getJobStatus().get().equals(JobStatus.RUNNING));

    // wait for some checkpoints to be written
    CloseableHttpClient httpClient = HttpClients.createDefault();
    String checkpointApiUrl =
        String.format(
            "http://%s:%s/jobs/%s/checkpoints",
            env.getConfiguration().get(RestOptions.ADDRESS),
            env.getConfiguration().get(RestOptions.PORT),
            jobClient.getJobID().toString());
    HttpGet request = new HttpGet(checkpointApiUrl);

    Awaitility
        .await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> {
            CloseableHttpResponse response = httpClient.execute(request);
            String json = EntityUtils.toString(response.getEntity());
            Checkpoints checkpoints = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(json, Checkpoints.class);

            return checkpoints != null && checkpoints.getCounts().getCompleted() > 0;
        });

    // save the job gracefully
    LOGGER.info("Stopping gracefully");
    jobClient.stopWithSavepoint(
        false,
        "/tmp/savepoint_" + UUID.randomUUID(),
        SavepointFormatType.DEFAULT
    ).get();

    // wait until job is finished
    Awaitility.await().until(() -> jobClient.getJobStatus().get().equals(JobStatus.FINISHED));
    LOGGER.info("Application finished");

    // manually call listeners because we need to run executeAsync to gracefully finish but
    // listener is called only on execute
    Field field = FieldUtils.getField(StreamExecutionEnvironment.class, "jobListeners", true);
    List<JobListener> jobListeners = (List<JobListener>) field.get(env);

    LOGGER.info("calling onJobExecuted on listeners");
    jobListeners.forEach(
        jobListener -> {
            try {
                jobListener.onJobExecuted(jobClient.getJobExecutionResult().get(), null);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    );

    // wait another few secs to still check if tracker thread stopped emitting running events
    // checkpointing thread is triggered to run each second
    Thread.sleep(5000);
  }
}
