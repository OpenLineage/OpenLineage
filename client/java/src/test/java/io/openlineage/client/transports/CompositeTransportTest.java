/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.runEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.openlineage.client.OpenLineage;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

// Fake transport base class for testing
abstract class FakeTransport extends Transport {
  private boolean emitted;

  public boolean isEmitted() {
    return emitted;
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emitted = true; // Set to true when emit is called
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emitted = true; // Set to true when emit is called
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emitted = true; // Set to true when emit is called
  }
}

// FakeTransportA extending the base class
class FakeTransportA extends FakeTransport {}

// FakeTransportB extending the base class
class FakeTransportB extends FakeTransport {}

// Mock transport configuration class for FakeTransportA
class FakeTransportConfigA implements TransportConfig {}

// Mock transport configuration class for FakeTransportB
class FakeTransportConfigB implements TransportConfig {}

class CompositeTransportTest {

  private static FakeTransportA fakeTransportA;
  private static FakeTransportB fakeTransportB;
  private CompositeConfig compositeConfig;

  @BeforeEach
  void setUp() {
    // Initialize fake transports
    fakeTransportA = spy(new FakeTransportA());
    fakeTransportB = spy(new FakeTransportB());

    // Mock configuration for the CompositeTransport
    Map<String, Object> fakeTransportAConfig = new HashMap<>();
    fakeTransportAConfig.put("type", "fakeA");
    Map<String, Object> fakeTransportBConfig = new HashMap<>();
    fakeTransportBConfig.put("type", "fakeB");

    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      compositeConfig =
          new CompositeConfig(
              Arrays.asList(fakeTransportAConfig, fakeTransportBConfig), true, true);
    }
  }

  public static MockedStatic<TransportResolver> mockTransportResolver() {
    MockedStatic<TransportResolver> mockedStatic = Mockito.mockStatic(TransportResolver.class);

    mockedStatic
        .when(() -> TransportResolver.resolveTransportConfigByType("fakeA"))
        .thenReturn((Class<? extends TransportConfig>) FakeTransportConfigA.class);

    mockedStatic
        .when(() -> TransportResolver.resolveTransportConfigByType("fakeB"))
        .thenReturn((Class<? extends TransportConfig>) FakeTransportConfigB.class);

    mockedStatic
        .when(() -> TransportResolver.resolveTransportByConfig(any(FakeTransportConfigA.class)))
        .thenReturn(fakeTransportA);

    mockedStatic
        .when(() -> TransportResolver.resolveTransportByConfig(any(FakeTransportConfigB.class)))
        .thenReturn(fakeTransportB);

    return mockedStatic;
  }

  @Test
  void testEmitSuccessful() {
    OpenLineage.RunEvent event = runEvent();

    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      CompositeTransport compositeTransport = new CompositeTransport(compositeConfig);
      compositeTransport.emit(event);

      assertTrue(fakeTransportA.isEmitted());
      assertTrue(fakeTransportB.isEmitted());
      verify(fakeTransportA, times(1)).emit(event);
      verify(fakeTransportB, times(1)).emit(event);
    }
  }

  @Test
  void testEmitPartialFailureContinueOnFailureTrue() {
    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      CompositeTransport compositeTransport = new CompositeTransport(compositeConfig);
      doThrow(new RuntimeException("FakeTransportA failed"))
          .when(fakeTransportA)
          .emit(any(OpenLineage.RunEvent.class));

      OpenLineage.RunEvent event = runEvent();

      compositeTransport.emit(event);

      assertTrue(fakeTransportB.isEmitted()); // FakeTransportB should still emit
      verify(fakeTransportA, times(1)).emit(event);
      verify(fakeTransportB, times(1)).emit(event);
    }
  }

  @Test
  void testEmitPartialFailureContinueOnFailureFalse() {
    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      CompositeConfig configWithFailFast =
          CompositeConfig.createFromTransportConfigs(compositeConfig.getTransports(), false, true);
      CompositeTransport compositeTransport = new CompositeTransport(configWithFailFast);
      doThrow(new RuntimeException("FakeTransportA failed"))
          .when(fakeTransportA)
          .emit(any(OpenLineage.RunEvent.class));

      OpenLineage.RunEvent event = runEvent();

      RuntimeException exception =
          assertThrows(RuntimeException.class, () -> compositeTransport.emit(event));
      assertThat(exception.getMessage())
          .contains(("Transport FakeTransportA failed to emit event"));
      verify(fakeTransportA, times(1)).emit(event);
      verify(fakeTransportB, times(0)).emit(event);
    }
  }

  @Test
  void testEmitAllFailure() {
    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      CompositeTransport compositeTransport = new CompositeTransport(compositeConfig);
      doThrow(new RuntimeException("FakeTransportA failed"))
          .when(fakeTransportA)
          .emit(any(OpenLineage.RunEvent.class));
      doThrow(new RuntimeException("FakeTransportB failed"))
          .when(fakeTransportB)
          .emit(any(OpenLineage.RunEvent.class));

      OpenLineage.RunEvent event = runEvent();

      compositeTransport.emit(event);

      verify(fakeTransportA, times(1)).emit(event);
      verify(fakeTransportB, times(1)).emit(event);
    }
  }

  @Test
  void testInvalidConfig() {
    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      Map<String, Object> invalidConfig = new HashMap<>();
      invalidConfig.put("type", "INVALID");
      // Mock behavior for invalid type
      mockedStatic
          .when(() -> TransportResolver.resolveTransportConfigByType("INVALID"))
          .thenThrow(new IllegalArgumentException("Invalid transport"));

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> new CompositeConfig(Arrays.asList(invalidConfig), true, true));
      assertTrue(exception.getMessage().contains("Invalid transport"));
    }
  }

  @Test
  void testConfigWithMap() {
    try (MockedStatic<TransportResolver> mockedStatic = mockTransportResolver()) {
      Map<String, Object> config = new HashMap<>();
      Map<String, Object> fakeTransportAConfig = new HashMap<>();
      fakeTransportAConfig.put("type", "fakeA");
      Map<String, Object> fakeTransportBConfig = new HashMap<>();
      fakeTransportBConfig.put("type", "fakeB");
      config.put("myFakeA", fakeTransportAConfig);
      config.put("myFakeB", fakeTransportBConfig);
      CompositeConfig compositeConfig = new CompositeConfig(config, true, true);
      assertEquals(compositeConfig.getTransports().get(0).getName(), "myFakeA");
      assertEquals(compositeConfig.getTransports().get(1).getName(), "myFakeB");
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testParallelEmissionOfRunEvents(String withThreadPool) {
    AtomicInteger eventsEmitted = new AtomicInteger(0);
    try (MockedStatic<TransportResolver> mockedStatic =
        Mockito.mockStatic(TransportResolver.class)) {
      mockedStatic
          .when(() -> TransportResolver.resolveTransportConfigByType(any()))
          .thenReturn((Class<? extends TransportConfig>) FakeTransportConfigA.class);

      mockedStatic
          .when(() -> TransportResolver.resolveTransportByConfig(any()))
          .thenReturn(new FakeTransportWithSleep(100, eventsEmitted));

      Map<String, Object> config = new HashMap<>();
      Map<String, Object> fakeTransportConfig = new HashMap<>();
      fakeTransportConfig.put("type", "fakeA");
      IntStream.range(0, 10)
          .forEach(
              i -> {
                config.put("myFakeA" + i, fakeTransportConfig);
              });

      compositeConfig = new CompositeConfig(config, true, Boolean.parseBoolean(withThreadPool));
      CompositeTransport compositeTransport = new CompositeTransport(compositeConfig);

      long startTime;
      long endTime;
      // Verify RunEvent emission
      startTime = System.currentTimeMillis();
      compositeTransport.emit(runEvent());
      endTime = System.currentTimeMillis();

      assertThat(eventsEmitted.get()).isEqualTo(10); // All events should be emitted
      assertThat(endTime - startTime)
          .isGreaterThanOrEqualTo(100)
          .isLessThan(200); // Should take around 100ms to emit all events

      // Verify DatasetEvent emission
      startTime = System.currentTimeMillis();
      compositeTransport.emit(runEvent());
      endTime = System.currentTimeMillis();

      assertThat(eventsEmitted.get()).isEqualTo(20); // All events should be emitted
      assertThat(endTime - startTime)
          .isGreaterThanOrEqualTo(100)
          .isLessThan(200); // Should take around 100ms to emit all events

      // Verify JobEvent emission
      startTime = System.currentTimeMillis();
      compositeTransport.emit(runEvent());
      endTime = System.currentTimeMillis();

      assertThat(eventsEmitted.get()).isEqualTo(30); // All events should be emitted
      assertThat(endTime - startTime)
          .isGreaterThan(100)
          .isLessThan(200); // Should take around 100ms to emit all events

      // Verify thread pool not shutdown
      compositeTransport.emit(runEvent());
      assertThat(eventsEmitted.get()).isEqualTo(40); // All events should be emitted
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testShutdownThreadPool(String withThreadPoolString) {
    boolean withThreadPool = Boolean.parseBoolean(withThreadPoolString);
    try (MockedStatic<TransportResolver> mockedStatic =
        Mockito.mockStatic(TransportResolver.class)) {
      try (MockedStatic<Executors> mockedExecutors = Mockito.mockStatic(Executors.class)) {
        mockedStatic
            .when(() -> TransportResolver.resolveTransportConfigByType(any()))
            .thenReturn((Class<? extends TransportConfig>) FakeTransportConfigA.class);

        mockedStatic
            .when(() -> TransportResolver.resolveTransportByConfig(any()))
            .thenReturn(new FakeTransport() {});

        ExecutorService threadPool = mock(ExecutorService.class);
        mockedExecutors.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(threadPool);

        Map<String, Object> config = new HashMap<>();
        Map<String, Object> fakeTransportConfig = new HashMap<>();
        fakeTransportConfig.put("type", "fakeA");
        config.put("myFakeA", fakeTransportConfig);

        compositeConfig = new CompositeConfig(config, true, withThreadPool);
        CompositeTransport compositeTransport = new CompositeTransport(compositeConfig);

        compositeTransport.emit(runEvent());

        // the other emit verifies we don't use shutdown pool
        compositeTransport.emit(runEvent());

        if (withThreadPool) {
          // verify thread pool should not shut down
          verify(threadPool, times(0)).shutdown();
        } else {
          // verify thread pool gets shut down
          verify(threadPool, times(2)).shutdown();
        }
      }
    }
  }

  private static class FakeTransportWithSleep extends FakeTransport {
    private final long sleepTime;
    private final AtomicInteger emittedCounter;

    public FakeTransportWithSleep(long sleepTime, AtomicInteger emittedCounter) {
      super();
      this.sleepTime = sleepTime;
      this.emittedCounter = emittedCounter;
    }

    @Override
    @SneakyThrows
    public void emit(@NonNull OpenLineage.RunEvent runEvent) {
      Thread.sleep(sleepTime);
      emittedCounter.incrementAndGet();
    }

    @Override
    @SneakyThrows
    public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
      Thread.sleep(sleepTime);
      emittedCounter.incrementAndGet();
    }

    @Override
    @SneakyThrows
    public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
      Thread.sleep(sleepTime);
      emittedCounter.incrementAndGet();
    }
  }
}
