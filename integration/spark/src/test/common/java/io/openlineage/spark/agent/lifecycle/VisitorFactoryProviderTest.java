package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.apache.spark.SparkContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VisitorFactoryProviderTest {
  SparkContext sparkContext = mock(SparkContext.class);

  @ParameterizedTest
  @MethodSource("provideVersionFactory")
  void getInstance(String version, String expectedClass) {
    when(sparkContext.version()).thenReturn(version);
    VisitorFactory factory = VisitorFactoryProvider.getInstance(sparkContext);
    assertThat(factory.getClass().getName()).isEqualTo(expectedClass);
  }

  private static Stream<Arguments> provideVersionFactory() {
    return Stream.of(
        Arguments.of("3.1", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("3.0", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("4.0", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("1.0", "io.openlineage.spark.agent.lifecycle.Spark3VisitorFactoryImpl"),
        Arguments.of("2.4", "io.openlineage.spark.agent.lifecycle.Spark2VisitorFactoryImpl"),
        Arguments.of("2.7", "io.openlineage.spark.agent.lifecycle.Spark2VisitorFactoryImpl"));
  }
}
