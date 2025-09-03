/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.TransportConfig;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link OpenLineageClientUtils}. */
class OpenLineageClientUtilsTest {
  private static final String VALUE = "test";
  private static final Object OBJECT = new Object(VALUE);
  private static final TypeReference<Object> TYPE = new TypeReference<Object>() {};
  private static final String JSON = "{\"value\":\"" + VALUE + "\"}";

  @BeforeEach
  public void setUp() {
    OpenLineageClientUtils.configureObjectMapper(new String[] {});
  }

  @Test
  void testToJson() {
    final String actual = OpenLineageClientUtils.toJson(OBJECT);
    assertThat(actual).isEqualTo(JSON);
  }

  @Test
  void testToJson_withDisabledFacets() {
    OpenLineageClientUtils.configureObjectMapper(new String[] {"excludedValue"});
    final String actual = OpenLineageClientUtils.toJson(new ObjectWithDisabledFacets("a", "b"));

    assertThat(actual).contains("notExcludedValue");
    assertThat(actual).doesNotContain("excludedValue");
  }

  @Test
  void testToJson_withDisabledFacetsIsNull() {
    OpenLineageClientUtils.configureObjectMapper();
    final String actual = OpenLineageClientUtils.toJson(new ObjectWithDisabledFacets("a", "b"));

    assertThat(actual).contains("notExcludedValue");
    assertThat(actual).contains("excludedValue");
  }

  @Test
  void testToJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.toJson(null));
  }

  @Test
  void testFromJson() {
    final Object actual = OpenLineageClientUtils.fromJson(JSON, TYPE);
    assertThat(actual).isEqualToComparingFieldByField(OBJECT);
  }

  @Test
  void testFromJson_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.fromJson(JSON, null));
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.fromJson(null, TYPE));
  }

  @Test
  void testToUrl() throws Exception {
    final String urlString = "http://test.com:8080";
    final URI expected = new URI(urlString);
    final URI actual = OpenLineageClientUtils.toUri(urlString);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testToUrl_throwsOnNull() {
    assertThatNullPointerException().isThrownBy(() -> OpenLineageClientUtils.toUri(null));
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  static final class Object {
    final String value;

    @JsonCreator
    Object(@JsonProperty("value") final String value) {
      this.value = value;
    }
  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  static final class ObjectWithDisabledFacets {
    final String excludedValue;
    final String notExcludedValue;

    @JsonCreator
    ObjectWithDisabledFacets(
        @JsonProperty("excludedValue") final String excludedValue,
        @JsonProperty("notExcludedValue") final String notExcludedValue) {
      this.excludedValue = excludedValue;
      this.notExcludedValue = notExcludedValue;
    }
  }

  @Test
  void loadOpenLineageYaml_shouldDeserialiseYamlEncodedInputStreams() {
    String yamlString =
        "transport:\n"
            + "  type: http\n"
            + "  url: http://localhost:5050\n"
            + "  endpoint: api/v1/lineage\n"
            + "  compression: gzip\n";

    byte[] bytes = yamlString.getBytes(StandardCharsets.UTF_8);

    OpenLineageConfig openLineageConfig =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new ByteArrayInputStream(bytes), new TypeReference<OpenLineageConfig>() {});
    TransportConfig transportConfig = openLineageConfig.getTransportConfig();
    assertThat(transportConfig).isNotNull();
    assertThat(transportConfig).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) transportConfig;
    assertThat(httpConfig.getUrl()).isEqualTo(URI.create("http://localhost:5050"));
    assertThat(httpConfig.getEndpoint()).isEqualTo("api/v1/lineage");
    assertThat(httpConfig.getCompression()).isEqualTo(HttpConfig.Compression.GZIP);
  }

  @Test
  void loadOpenLineageYaml_shouldFallbackAndDeserialiseJsonEncodedInputStreams() {
    byte[] bytes =
        "{\"transport\":{\"type\":\"http\",\"url\":\"https://localhost:1234/api/v1/lineage\",\"compression\":\"gzip\"}}"
            .getBytes(StandardCharsets.UTF_8);

    OpenLineageConfig openLineageConfig =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new ByteArrayInputStream(bytes), new TypeReference<OpenLineageConfig>() {});
    TransportConfig transportConfig = openLineageConfig.getTransportConfig();
    assertThat(transportConfig).isNotNull();
    assertThat(transportConfig).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) transportConfig;
    assertThat(httpConfig.getUrl()).isEqualTo(URI.create("https://localhost:1234/api/v1/lineage"));
    assertThat(httpConfig.getCompression()).isEqualTo(HttpConfig.Compression.GZIP);
  }

  @Test
  void loadOpenLineageJson_ShouldDeserialiseJsonEncodedInputStreams() {
    byte[] bytes =
        "{\"transport\":{\"type\":\"http\",\"url\":\"https://localhost:1234/api/v1/lineage\",\"compression\":\"gzip\"}}"
            .getBytes(StandardCharsets.UTF_8);

    OpenLineageConfig openLineageConfig =
        OpenLineageClientUtils.loadOpenLineageConfigJson(
            new ByteArrayInputStream(bytes), new TypeReference<OpenLineageConfig>() {});
    TransportConfig transportConfig = openLineageConfig.getTransportConfig();
    assertThat(transportConfig).isNotNull();
    assertThat(transportConfig).isInstanceOf(HttpConfig.class);
    HttpConfig httpConfig = (HttpConfig) transportConfig;
    assertThat(httpConfig.getUrl()).isEqualTo(URI.create("https://localhost:1234/api/v1/lineage"));
    assertThat(httpConfig.getCompression()).isEqualTo(HttpConfig.Compression.GZIP);
  }

  @Test
  void testGetOrCreateExecutor_createsFixedThreadPool() {
    // Clear any existing executor first
    clearExecutor();

    ExecutorService executor = OpenLineageClientUtils.getOrCreateExecutor();

    assertThat(executor).isNotNull();
    assertThat(executor).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
    // FixedThreadPool has core and max pool size equal
    assertThat(threadPool.getCorePoolSize())
        .isEqualTo(OpenLineageClientUtils.DEFAULT_THREAD_POOL_SIZE);
    assertThat(threadPool.getMaximumPoolSize())
        .isEqualTo(OpenLineageClientUtils.DEFAULT_THREAD_POOL_SIZE);
  }

  @Test
  void testGetOrCreateExecutor_returnsSameInstanceOnMultipleCalls() {
    // Clear any existing executor first
    clearExecutor();

    ExecutorService executor1 = OpenLineageClientUtils.getOrCreateExecutor();
    ExecutorService executor2 = OpenLineageClientUtils.getOrCreateExecutor();

    assertThat(executor1).isSameAs(executor2);
  }

  @Test
  void testGetOrCreateExecutor_withCustomThreadFactory() {
    // Clear any existing executor first
    clearExecutor();

    ThreadFactory customFactory =
        r -> {
          Thread t = new Thread(r);
          t.setName("custom-thread");
          return t;
        };

    ExecutorService executor = OpenLineageClientUtils.getOrCreateExecutor(customFactory);

    assertThat(executor).isNotNull();
    assertThat(executor).isInstanceOf(ThreadPoolExecutor.class);

    ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
    assertThat(threadPool.getCorePoolSize())
        .isEqualTo(OpenLineageClientUtils.DEFAULT_THREAD_POOL_SIZE);
    assertThat(threadPool.getMaximumPoolSize())
        .isEqualTo(OpenLineageClientUtils.DEFAULT_THREAD_POOL_SIZE);
  }

  @Test
  void testGetOrCreateExecutor_threadSafety() throws InterruptedException {
    // Clear any existing executor first
    clearExecutor();

    int threadCount = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(threadCount);
    @SuppressWarnings("unchecked")
    AtomicReference<ExecutorService>[] results = new AtomicReference[threadCount];

    // Initialize the array
    for (int i = 0; i < threadCount; i++) {
      results[i] = new AtomicReference<>();
    }

    // Create multiple threads that will call getOrCreateExecutor simultaneously
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      new Thread(
              () -> {
                try {
                  startLatch.await(); // Wait for all threads to be ready
                  ExecutorService executor = OpenLineageClientUtils.getOrCreateExecutor();
                  results[index].set(executor);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  endLatch.countDown();
                }
              })
          .start();
    }

    startLatch.countDown(); // Start all threads simultaneously
    assertThat(endLatch.await(5, TimeUnit.SECONDS)).isTrue();

    // All threads should get the same executor instance
    ExecutorService firstResult = results[0].get();
    assertThat(firstResult).isNotNull();

    for (int i = 1; i < threadCount; i++) {
      assertThat(results[i].get()).isSameAs(firstResult);
    }
  }

  @Test
  void testGetExecutor_whenNotCreated() {
    // Clear any existing executor first
    clearExecutor();

    Optional<ExecutorService> executor = OpenLineageClientUtils.getExecutor();

    assertThat(executor).isEmpty();
  }

  @Test
  void testGetExecutor_afterCreation() {
    // Clear any existing executor first
    clearExecutor();

    ExecutorService created = OpenLineageClientUtils.getOrCreateExecutor();
    Optional<ExecutorService> retrieved = OpenLineageClientUtils.getExecutor();

    assertThat(retrieved).isPresent();
    assertThat(retrieved.get()).isSameAs(created);
  }

  @Test
  void testExecutorThreadFactory_createsNamedThreads() {
    OpenLineageClientUtils.ExecutorThreadFactory factory =
        new OpenLineageClientUtils.ExecutorThreadFactory("test-prefix");

    Thread thread1 = factory.newThread(() -> {});
    Thread thread2 = factory.newThread(() -> {});

    assertThat(thread1.getName()).startsWith("test-prefix-");
    assertThat(thread2.getName()).startsWith("test-prefix-");
    assertThat(thread1.getName()).isNotEqualTo(thread2.getName());

    // Verify thread properties
    assertThat(thread1.isDaemon()).isFalse();
    assertThat(thread1.getPriority()).isEqualTo(Thread.NORM_PRIORITY);
  }

  /**
   * Helper method to clear the static executor for test isolation. Uses reflection to access the
   * private static field.
   */
  @SuppressWarnings({"unchecked", "PMD.AvoidAccessibilityAlteration"})
  private void clearExecutor() {
    try {
      java.lang.reflect.Field executorField =
          OpenLineageClientUtils.class.getDeclaredField("EXECUTOR");
      executorField.setAccessible(true);
      AtomicReference<ExecutorService> executorRef =
          (AtomicReference<ExecutorService>) executorField.get(null);
      ExecutorService existing = executorRef.getAndSet(null);
      if (existing != null) {
        existing.shutdown();
      }
    } catch (Exception e) {
      // Ignore reflection errors in tests
    }
  }
}
