/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.circuitBreaker.StaticCircuitBreakerConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolverConfig;
import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.CompositeConfig;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.KafkaConfig;
import io.openlineage.client.utils.TagField;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig.VendorsConfig.VendorConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ArgumentParserTest {

  private static final String NS_NAME = "ns_name";
  private static final String JOB_NAMESPACE = "job_namespace";
  private static final String JOB_NAME = "job_name";
  private static final String URL = "http://localhost:5000";
  private static final String RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";
  private static final String ROOT_PARENT_JOB_NAME = "root-name";
  private static final String ROOT_PARENT_JOB_NAMESPACE = "root-namespace";
  private static final String ROOT_PARENT_RUN_ID = "ea445b5c-1a1a-2b2b-3c3c-01c7c52b6e54";
  private static final String APP_NAME = "test";
  private static final String APP_RUN_ID = "ea445b5c-22eb-457a-8007-01c7c52b6e54";
  private static final String ENDPOINT = "api/v1/lineage";
  private static final String AUTH_TYPE = "api_key";
  private static final String API_KEY = "random_token";
  private SparkOpenLineageConfig config;
  private static final String TEST_TOKEN = "TOKEN";

  @Test
  @SuppressWarnings({"deprecation"})
  void testDefaults() {
    config = ArgumentParser.parse(new SparkConf());
    assertThat(config.getFacetsConfig().getDisabledFacets())
        .containsAllEntriesOf(
            ImmutableMap.of("spark_unknown", true, "spark.logicalPlan", true, "debug", true));
    assertThat(config.getTransportConfig()).isInstanceOf(ConsoleConfig.class);
  }

  @Test
  void testTransportTypes() {
    config =
        ArgumentParser.parse(
            new SparkConf().set(ArgumentParser.SPARK_CONF_TRANSPORT_TYPE, "console"));
    SparkOpenLineageConfig configHttp =
        ArgumentParser.parse(new SparkConf().set(ArgumentParser.SPARK_CONF_TRANSPORT_TYPE, "http"));
    SparkOpenLineageConfig configKafka =
        ArgumentParser.parse(
            new SparkConf().set(ArgumentParser.SPARK_CONF_TRANSPORT_TYPE, "kafka"));

    assertThat(config.getTransportConfig()).isInstanceOf(ConsoleConfig.class);
    assertThat(configHttp.getTransportConfig()).isInstanceOf(HttpConfig.class);
    assertThat(configKafka.getTransportConfig()).isInstanceOf(KafkaConfig.class);
  }

  @Test
  void testLoadingSparkConfig() {
    SparkConf sparkConf =
        new SparkConf()
            .set(ArgumentParser.SPARK_CONF_NAMESPACE, NS_NAME)
            .set(ArgumentParser.SPARK_CONF_PARENT_JOB_NAMESPACE, JOB_NAMESPACE)
            .set(ArgumentParser.SPARK_CONF_PARENT_JOB_NAME, JOB_NAME)
            .set(ArgumentParser.SPARK_CONF_PARENT_RUN_ID, RUN_ID)
            .set("spark.openlineage.rootParentJobName", ROOT_PARENT_JOB_NAME)
            .set("spark.openlineage.rootParentJobNamespace", ROOT_PARENT_JOB_NAMESPACE)
            .set("spark.openlineage.rootParentRunId.", ROOT_PARENT_RUN_ID)
            .set(ArgumentParser.SPARK_CONF_APP_NAME, APP_NAME)
            .set(ArgumentParser.SPARK_CONF_APP_RUN_ID, APP_RUN_ID);

    config = ArgumentParser.parse(sparkConf);
    assertEquals(JOB_NAMESPACE, config.getParentJobNamespace());
    assertEquals(NS_NAME, config.getNamespace());
    assertEquals(JOB_NAME, config.getParentJobName());
    assertEquals(RUN_ID, config.getParentRunId());
    assertEquals(APP_NAME, config.getOverriddenAppName());
    assertEquals(APP_RUN_ID, config.getOverriddenApplicationRunId());

    assertEquals(ROOT_PARENT_JOB_NAME, config.getRootParentJobName());
    assertEquals(ROOT_PARENT_JOB_NAMESPACE, config.getRootParentJobNamespace());
    assertEquals(ROOT_PARENT_RUN_ID, config.getRootParentRunId());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  void testConfToHttpConfig() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.transport.type", "http")
            .set("spark.openlineage.transport.url", URL)
            .set("spark.openlineage.transport.endpoint", ENDPOINT)
            .set("spark.openlineage.transport.auth.type", AUTH_TYPE)
            .set("spark.openlineage.transport.auth.apiKey", API_KEY)
            .set("spark.openlineage.transport.timeoutInMillis", "5000")
            .set("spark.openlineage.transport.urlParams.test1", "test1")
            .set("spark.openlineage.transport.urlParams.test2", "test2")
            .set("spark.openlineage.transport.headers.testHeader1", "test1")
            .set("spark.openlineage.transport.headers.testHeader2", "test2")
            .set("spark.openlineage.transport.compression", "gzip");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    HttpConfig transportConfig = (HttpConfig) config.getTransportConfig();
    assertEquals(URL, transportConfig.getUrl().toString());
    assertEquals(ENDPOINT, transportConfig.getEndpoint());
    assert (transportConfig.getAuth() != null);
    assert (transportConfig.getAuth() instanceof ApiKeyTokenProvider);
    assertEquals("Bearer random_token", transportConfig.getAuth().getToken());
    assertEquals(5000, transportConfig.getTimeoutInMillis());
    assertEquals("test1", transportConfig.getHeaders().get("testHeader1"));
    assertEquals("test2", transportConfig.getHeaders().get("testHeader2"));
    assertEquals(HttpConfig.Compression.GZIP, transportConfig.getCompression());
  }

  @Test
  void testConfToKafkaConfig() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.transport.type", "kafka")
            .set("spark.openlineage.transport.topicName", "test")
            .set("spark.openlineage.transport.messageKey", "explicit-key")
            .set("spark.openlineage.transport.properties.test1", "test1")
            .set("spark.openlineage.transport.properties.test2", "test2");
    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    KafkaConfig transportConfig = (KafkaConfig) config.getTransportConfig();
    assertEquals("test", transportConfig.getTopicName());
    assertEquals("explicit-key", transportConfig.getMessageKey());
    assertEquals("test1", transportConfig.getProperties().get("test1"));
    assertEquals("test2", transportConfig.getProperties().get("test2"));
  }

  @Test
  void testLoadConfigFromEnvVars() {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      Map<String, String> sparkConfig = new HashMap<>();

      sparkConfig.put("OPENLINEAGE__TRANSPORT__TYPE", "composite");
      sparkConfig.put("OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE", "true");
      sparkConfig.put("OPENLINEAGE__TRANSPORT__TRANSPORTS__KAVVKA__TYPE", "kafka");
      sparkConfig.put("OPENLINEAGE__TRANSPORT__TRANSPORTS__KAVVKA__TOPIC_NAME", "test");
      sparkConfig.put("OPENLINEAGE__TRANSPORT__TRANSPORTS__KAVVKA__MESSAGE_KEY", "explicit-key");
      sparkConfig.put(
          "OPENLINEAGE__TRANSPORT__TRANSPORTS__KAVVKA__PROPERTIES",
          "{\"test1\": \"test1\", \"test2\": \"test2\"}");
      sparkConfig.put("OPENLINEAGE__TRANSPORT__TRANSPORTS__LOCAL__TYPE", "http");
      sparkConfig.put(
          "OPENLINEAGE__TRANSPORT__TRANSPORTS__LOCAL__URL",
          "http://your-openlineage-endpoint/api/v1/lineage");
      when(Environment.getAllEnvironmentVariables()).thenReturn(sparkConfig);

      SparkConf sparkConf = new SparkConf();
      SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
      CompositeConfig transportConfig = (CompositeConfig) config.getTransportConfig();
      assertEquals(
          "http://your-openlineage-endpoint/api/v1/lineage",
          ((HttpConfig) transportConfig.getTransports().get(1)).getUrl().toString());
      KafkaConfig kafkaConfig = (KafkaConfig) transportConfig.getTransports().get(0);
      assertEquals("test", kafkaConfig.getTopicName());
      assertEquals("explicit-key", kafkaConfig.getMessageKey());
      assertEquals("test1", kafkaConfig.getProperties().get("test1"));
      assertEquals("test2", kafkaConfig.getProperties().get("test2"));
      assertEquals("kavvka", kafkaConfig.getName());
    }
  }

  @Test
  void testLoadConfigPrecedence() {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getAllEnvironmentVariables())
          .thenReturn(Collections.singletonMap("OPENLINEAGE__TRANSPORT__TYPE", "console"));

      SparkConf sparkConf =
          new SparkConf()
              .set("spark.openlineage.transport.type", "http")
              .set("spark.openlineage.transport.url", URL);
      SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
      assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);
    }
  }

  @Test
  void testConfToCompositeTransport() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.transport.type", "composite")
            .set("spark.openlineage.transport.continueOnFailure", "true")
            .set("spark.openlineage.transport.transports.kavvka.type", "kafka")
            .set("spark.openlineage.transport.transports.kavvka.topicName", "test")
            .set("spark.openlineage.transport.transports.kavvka.messageKey", "explicit-key")
            .set("spark.openlineage.transport.transports.kavvka.properties.test1", "test1")
            .set("spark.openlineage.transport.transports.kavvka.properties.test2", "test2")
            .set("spark.openlineage.transport.transports.local.type", "http")
            .set(
                "spark.openlineage.transport.transports.local.url",
                "http://your-openlineage-endpoint/api/v1/lineage");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    CompositeConfig transportConfig = (CompositeConfig) config.getTransportConfig();
    assertEquals(
        "http://your-openlineage-endpoint/api/v1/lineage",
        ((HttpConfig) transportConfig.getTransports().get(1)).getUrl().toString());
    KafkaConfig kafkaConfig = (KafkaConfig) transportConfig.getTransports().get(0);
    assertEquals("test", kafkaConfig.getTopicName());
    assertEquals("explicit-key", kafkaConfig.getMessageKey());
    assertEquals("test1", kafkaConfig.getProperties().get("test1"));
    assertEquals("test2", kafkaConfig.getProperties().get("test2"));
    assertEquals("kavvka", kafkaConfig.getName());
  }

  @Test
  void testCircuitBreakerConfig() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.circuitBreaker.type", "static")
            .set("spark.openlineage.circuitBreaker.valuesReturned", "false,true");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    assertThat(config.getCircuitBreaker()).isInstanceOf(StaticCircuitBreakerConfig.class);
    assertThat(((StaticCircuitBreakerConfig) config.getCircuitBreaker()).getValuesReturned())
        .isEqualTo("false,true");
  }

  @Test
  @SuppressWarnings({"deprecation", "UnstableApiUsage", "ConstantConditions"})
  void testConfigReadFromYamlFile() {
    String propertyBefore = System.getProperty("user.dir");
    System.setProperty("user.dir", Resources.getResource("config").getPath());

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.openlineage.run.tags", "overwrite:overwritten");
    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    System.setProperty("user.dir", propertyBefore);

    assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);

    HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();
    assertThat(httpConfig.getUrl().toString()).isEqualTo("http://localhost:1010");
    assertThat(httpConfig.getAuth().getToken()).isEqualTo("Bearer random_token");
    assertThat(config.getJobName().getAppendDatasetName()).isFalse();
    assertThat(config.getFacetsConfig().getDisabledFacets())
        .containsAllEntriesOf(
            ImmutableMap.of("spark_unknown", false, "spark.logicalPlan", true, "debug", true));
    assertThat(config.getJobConfig().getTags())
        .contains(new TagField("key", "value"), new TagField("tag2"));
    assertThat(config.getRunConfig().getTags())
        .contains(
            new TagField("something", "will", "be"), new TagField("overwrite", "overwritten"));
  }

  @Test
  @SuppressWarnings({"UnstableApiUsage", "ConstantConditions"})
  void testSparkConfOverwritesFileBasedConfig() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.transport.type", "http")
            .set("spark.openlineage.transport.url", URL);

    String propertyBefore = System.getProperty("user.dir");
    System.setProperty("user.dir", Resources.getResource("config").getPath());

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    System.setProperty("user.dir", propertyBefore);

    assertThat(config.getTransportConfig()).isInstanceOf(HttpConfig.class);

    HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();

    // URL overwritten by SparkConf
    assertThat(httpConfig.getUrl().toString()).isEqualTo(URL);

    // API config from yaml file
    assertThat(httpConfig.getAuth().getToken()).isEqualTo("Bearer random_token");
  }

  @Test
  void testConfToHttpConfigWithCustomTokenProvider() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.transport.type", "http")
            .set(
                "spark.openlineage.transport.auth.type", FakeTokenProvider.class.getCanonicalName())
            .set("spark.openlineage.transport.auth.token", TEST_TOKEN);
    SparkOpenLineageConfig openLineageYaml = ArgumentParser.parse(sparkConf);
    HttpConfig transportConfig = (HttpConfig) openLineageYaml.getTransportConfig();
    assert (transportConfig.getAuth() != null);
    assert (transportConfig.getAuth() instanceof FakeTokenProvider);
    assertEquals(TEST_TOKEN, transportConfig.getAuth().getToken());
  }

  @Test
  void testConfToHttpConfigWithInvalidCustomTokenProvider() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.transport.type", "http")
            .set("spark.openlineage.transport.auth.type", "non.existing.TokenProvider")
            .set("spark.openlineage.transport.auth.token", TEST_TOKEN);

    Logger.getLogger(OpenLineageClientUtils.class).setLevel(Level.ERROR);
    assertThrows(RuntimeException.class, () -> ArgumentParser.parse(sparkConf));
  }

  @Test
  void testMultipleDatasetNamespaceResolverConfigEntries() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.dataset.namespaceResolvers.postgres-prod.type", "hostList")
            .set(
                "spark.openlineage.dataset.namespaceResolvers.postgres-prod.hosts",
                "[postgres-host1-prod;postgres-host1-prod]")
            .set("spark.openlineage.dataset.namespaceResolvers.postgres-test.type", "hostList")
            .set(
                "spark.openlineage.dataset.namespaceResolvers.postgres-test.hosts",
                "[postgres-host1-test;postgres-host1-test]");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);

    Map<String, DatasetNamespaceResolverConfig> namespaceResolvers =
        config.getDatasetConfig().getNamespaceResolvers();
    assertThat(namespaceResolvers.keySet()).hasSize(2);
    assertThat(namespaceResolvers.get("postgres-prod"))
        .hasFieldOrPropertyWithValue(
            "hosts", Arrays.asList("postgres-host1-prod", "postgres-host1-prod"));

    assertThat(namespaceResolvers.get("postgres-test"))
        .hasFieldOrPropertyWithValue(
            "hosts", Arrays.asList("postgres-host1-test", "postgres-host1-test"));
  }

  @Test
  void testNodeFilterConfiguration() {
    SparkConf sparkConf =
        new SparkConf()
            .set(
                "spark.openlineage.filter.allowedSparkNodes",
                "[org.apache.spark.sql.Node1;org.apache.spark.sql.Node1]")
            .set(
                "spark.openlineage.filter.deniedSparkNodes",
                "[org.apache.spark.sql.Node3;org.apache.spark.sql.Node4]");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);

    assertThat(config.getFilterConfig())
        .hasFieldOrPropertyWithValue(
            "allowedSparkNodes",
            Arrays.asList("org.apache.spark.sql.Node1", "org.apache.spark.sql.Node1"))
        .hasFieldOrPropertyWithValue(
            "deniedSparkNodes",
            Arrays.asList("org.apache.spark.sql.Node3", "org.apache.spark.sql.Node4"));
  }

  @Test
  void testExtractTags() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.job.tags", "tag;key:value;k:v:s;this:will:get:skipped")
            .set("spark.openlineage.run.tags", "otherTag;otherKey:otherValue");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);

    assertThat(config.getJobConfig().getTags())
        .isEqualTo(
            Arrays.asList(
                new TagField("tag"),
                new TagField("key", "value"),
                new TagField("k", "v", "s"),
                new TagField("this", "will", "get")));
    assertThat(config.getRunConfig().getTags())
        .isEqualTo(Arrays.asList(new TagField("otherTag"), new TagField("otherKey", "otherValue")));
  }

  @Test
  void testExtractTagsEscape() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.job.tags", "escaped\\:key:escaped\\;value;other:notescaped:tag")
            .set("spark.openlineage.run.tags", "\\;startkey\\;:endvalue\\;");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);

    assertThat(config.getJobConfig().getTags())
        .isEqualTo(
            Arrays.asList(
                new TagField("escaped:key", "escaped;value"),
                new TagField("other", "notescaped", "tag")));
    assertThat(config.getRunConfig().getTags())
        .isEqualTo(Arrays.asList(new TagField(";startkey;", "endvalue;")));
  }

  @Test
  void testExtractTagsEdgeCases() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.job.tags", "a:;:b")
            .set("spark.openlineage.run.tags", ";;:; : ;");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);

    assertThat(config.getJobConfig().getTags())
        .isEqualTo(Collections.singletonList(new TagField("a")));
    assertThat(config.getRunConfig().getTags()).isEmpty();
  }

  @Test
  void testExtractVendorConfig() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.openlineage.vendors.iceberg.metricsReporterDisabled", "true")
            .set("spark.openlineage.vendors.delta.metricsReporterDisabled", "false");

    SparkOpenLineageConfig config = ArgumentParser.parse(sparkConf);
    assertThat(config.getVendors().getConfig()).containsEntry("iceberg", new VendorConfig(true));
    assertThat(config.getVendors().getConfig()).containsEntry("delta", new VendorConfig(false));
  }
}
