/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.runEvent;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.openlineage.client.OpenLineage;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

// Fake transport base class for testing
abstract class FakeTransport extends Transport {
  private boolean emitted;

  public FakeTransport() {
    super(Type.NOOP); // Using NOOP or any placeholder type
  }

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

    compositeConfig =
        new CompositeConfig(Arrays.asList(fakeTransportAConfig, fakeTransportBConfig), true);
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
          new CompositeConfig(compositeConfig.getTransports(), false);
      CompositeTransport compositeTransport = new CompositeTransport(configWithFailFast);
      doThrow(new RuntimeException("FakeTransportA failed"))
          .when(fakeTransportA)
          .emit(any(OpenLineage.RunEvent.class));

      OpenLineage.RunEvent event = runEvent();

      RuntimeException exception =
          assertThrows(RuntimeException.class, () -> compositeTransport.emit(event));
      assertEquals("Transport FakeTransportA failed to emit event", exception.getMessage());
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
      CompositeConfig invalidCompositeConfig =
          new CompositeConfig(Arrays.asList(invalidConfig), true);
      // Mock behavior for invalid type
      mockedStatic
          .when(() -> TransportResolver.resolveTransportConfigByType("INVALID"))
          .thenThrow(new IllegalArgumentException("Invalid transport"));

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class, () -> new CompositeTransport(invalidCompositeConfig));
      assertTrue(exception.getMessage().contains("Invalid transport"));
    }
  }
}
