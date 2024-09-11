/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.flink.testutils;

import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.RetryRule;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base for the Kafka tests. It brings up:
 *
 * <ul>
 *   <li>A ZooKeeper mini cluster
 *   <li>Three Kafka Brokers (mini clusters)
 *   <li>A Flink mini cluster
 * </ul>
 *
 * <p>This code has been copied from flink-kafka-connector
 * https://github.com/apache/flink-connector-kafka/tree/main/flink-connector-kafka/src/test/java/org/apache/flink/connector/kafka/testutils
 *
 * <p>Code in this test is based on the following GitHub repository: <a
 * href="https://github.com/sakserv/hadoop-mini-clusters">
 * https://github.com/sakserv/hadoop-mini-clusters</a> (ASL licensed), as per commit
 * <i>bc6b2b2d5f6424d5f377aa6c0871e82a956462ef</i>
 *
 * <p>Tests inheriting from this class are known to be unstable due to the test setup. All tests
 * implemented in subclasses will be retried on failures.
 */
@SuppressWarnings({"serial", "PMD"})
@RetryOnFailure(times = 3)
public abstract class KafkaTestBase extends TestLogger {

  public static final Logger LOG = LoggerFactory.getLogger(KafkaTestBase.class);

  public static final int NUMBER_OF_KAFKA_SERVERS = 1;
  private static int numKafkaClusters = 1;

  public static String brokerConnectionStrings;

  public static Properties standardProps;

  public static KafkaTestEnvironment kafkaServer;

  public static List<KafkaClusterTestEnvMetadata> kafkaClusters = new ArrayList<>();

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  public static Properties secureProps = new Properties();

  @Rule public final RetryRule retryRule = new RetryRule();

  // ------------------------------------------------------------------------
  //  Setup and teardown of the mini clusters
  // ------------------------------------------------------------------------

  @BeforeClass
  public static void prepare() throws Exception {
    LOG.info("-------------------------------------------------------------------------");
    LOG.info("    Starting KafkaTestBase ");
    LOG.info("-------------------------------------------------------------------------");

    startClusters(false, numKafkaClusters);
  }

  @AfterClass
  public static void shutDownServices() throws Exception {

    LOG.info("-------------------------------------------------------------------------");
    LOG.info("    Shut down KafkaTestBase ");
    LOG.info("-------------------------------------------------------------------------");

    TestStreamEnvironment.unsetAsContext();

    shutdownClusters();

    LOG.info("-------------------------------------------------------------------------");
    LOG.info("    KafkaTestBase finished");
    LOG.info("-------------------------------------------------------------------------");
  }

  public static Configuration getFlinkConfiguration() {
    Configuration flinkConfig = new Configuration();
    flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
    return flinkConfig;
  }

  public static void startClusters() throws Exception {
    startClusters(
        KafkaTestEnvironment.createConfig().setKafkaServersNumber(NUMBER_OF_KAFKA_SERVERS));
  }

  public static void startClusters(boolean secureMode, int numKafkaClusters) throws Exception {
    startClusters(KafkaTestEnvironment.createConfig().setSecureMode(secureMode), numKafkaClusters);
  }

  public static void startClusters(
      KafkaTestEnvironment.Config environmentConfig, int numKafkaClusters) throws Exception {
    for (int i = 0; i < numKafkaClusters; i++) {
      startClusters(environmentConfig);
      KafkaClusterTestEnvMetadata kafkaClusterTestEnvMetadata =
          new KafkaClusterTestEnvMetadata(
              i, kafkaServer, standardProps, brokerConnectionStrings, secureProps);
      kafkaClusters.add(kafkaClusterTestEnvMetadata);
      LOG.info("Created Kafka cluster with configuration: {}", kafkaClusterTestEnvMetadata);
    }
  }

  public static void startClusters(KafkaTestEnvironment.Config environmentConfig) throws Exception {
    kafkaServer = constructKafkaTestEnvironment();

    LOG.info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.getVersion());

    kafkaServer.prepare(environmentConfig);

    standardProps = kafkaServer.getStandardProperties();

    brokerConnectionStrings = kafkaServer.getBrokerConnectionString();
  }

  public static KafkaTestEnvironment constructKafkaTestEnvironment() throws Exception {
    Class<?> clazz = Class.forName("io.openlineage.flink.testutils.KafkaTestEnvironmentImpl");
    return (KafkaTestEnvironment) InstantiationUtil.instantiate(clazz);
  }

  public static void shutdownClusters() throws Exception {
    if (secureProps != null) {
      secureProps.clear();
    }

    if (kafkaServer != null) {
      kafkaServer.shutdown();
    }

    if (kafkaClusters != null && !kafkaClusters.isEmpty()) {
      for (KafkaClusterTestEnvMetadata value : kafkaClusters) {
        value.getKafkaTestEnvironment().shutdown();
      }
      kafkaClusters.clear();
    }
  }

  // ------------------------------------------------------------------------
  //  Execution utilities
  // ------------------------------------------------------------------------

  public static void tryExecutePropagateExceptions(StreamExecutionEnvironment see, String name)
      throws Exception {
    try {
      see.execute(name);
    } catch (ProgramInvocationException | JobExecutionException root) {
      Throwable cause = root.getCause();

      // search for nested SuccessExceptions
      int depth = 0;
      while (!(cause instanceof SuccessException)) {
        if (cause == null || depth++ == 20) {
          throw root;
        } else {
          cause = cause.getCause();
        }
      }
    }
  }

  public static void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
    kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor);
  }

  public static void deleteTestTopic(String topic) {
    kafkaServer.deleteTestTopic(topic);
  }

  public static <K, V> void produceToKafka(
      Collection<ProducerRecord<K, V>> records,
      Class<? extends org.apache.kafka.common.serialization.Serializer<K>> keySerializerClass,
      Class<? extends org.apache.kafka.common.serialization.Serializer<V>> valueSerializerClass)
      throws Throwable {
    Properties props = new Properties();
    props.putAll(standardProps);
    props.putAll(kafkaServer.getIdempotentProducerConfig());
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());

    AtomicReference<Throwable> sendingError = new AtomicReference<>();
    Callback callback =
        (metadata, exception) -> {
          if (exception != null) {
            if (!sendingError.compareAndSet(null, exception)) {
              sendingError.get().addSuppressed(exception);
            }
          }
        };
    try (KafkaProducer<K, V> producer = new KafkaProducer<>(props)) {
      for (ProducerRecord<K, V> record : records) {
        producer.send(record, callback);
      }
    }
    if (sendingError.get() != null) {
      throw sendingError.get();
    }
  }

  private String formatElements(List<Integer> elements) {
    if (elements.size() > 50) {
      return String.format("number of elements: <%s>", elements.size());
    } else {
      return String.format("elements: <%s>", elements);
    }
  }

  public static void setNumKafkaClusters(int size) {
    numKafkaClusters = size;
  }

  /** Metadata generated by this test utility. */
  public static class KafkaClusterTestEnvMetadata {

    private final String kafkaClusterId;
    private final KafkaTestEnvironment kafkaTestEnvironment;
    private final Properties standardProperties;
    private final String brokerConnectionStrings;
    private final Properties secureProperties;

    private KafkaClusterTestEnvMetadata(
        int kafkaClusterIdx,
        KafkaTestEnvironment kafkaTestEnvironment,
        Properties standardProperties,
        String brokerConnectionStrings,
        Properties secureProperties) {
      this.kafkaClusterId = "kafka-cluster-" + kafkaClusterIdx;
      this.kafkaTestEnvironment = kafkaTestEnvironment;
      this.standardProperties = standardProperties;
      this.brokerConnectionStrings = brokerConnectionStrings;
      this.secureProperties = secureProperties;
    }

    public String getKafkaClusterId() {
      return kafkaClusterId;
    }

    public KafkaTestEnvironment getKafkaTestEnvironment() {
      return kafkaTestEnvironment;
    }

    public Properties getStandardProperties() {
      return standardProperties;
    }

    public String getBrokerConnectionStrings() {
      return brokerConnectionStrings;
    }

    public Properties getSecureProperties() {
      return secureProperties;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("kafkaClusterId", kafkaClusterId)
          .add("kafkaTestEnvironment", kafkaTestEnvironment)
          .add("standardProperties", standardProperties)
          .add("brokerConnectionStrings", brokerConnectionStrings)
          .add("secureProperties", secureProperties)
          .toString();
    }
  }
}
