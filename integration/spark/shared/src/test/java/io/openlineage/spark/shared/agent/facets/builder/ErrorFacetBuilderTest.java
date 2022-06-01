/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.facets.builder;

import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.shared.agent.facets.ErrorFacet;
import io.openlineage.spark.shared.agent.facets.builder.ErrorFacetBuilder;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorFacetBuilderTest {

  @Test
  public void testBuildErrorFacet() {
    JobFailed failure = new JobFailed(new TestException("The test exception message"));
    SparkListenerJobEnd event = new SparkListenerJobEnd(1, 1L, failure);
    ErrorFacetBuilder builder = new ErrorFacetBuilder();
    assertThat(builder.isDefinedAt(event)).isTrue();
    Map<String, RunFacet> runFacetMap = new HashMap<>();
    builder.build(event, runFacetMap::put);
    assertThat(runFacetMap)
        .hasEntrySatisfying(
            "spark.exception",
            facet ->
                assertThat(facet)
                    .isInstanceOf(ErrorFacet.class)
                    .extracting("message")
                    .isEqualTo(failure.exception().getMessage()));
  }

  @Test
  public void testIsUndefinedForJobSuccess() {
    SparkListenerJobEnd event = new SparkListenerJobEnd(1, 1L, JobSucceeded$.MODULE$);
    assertThat(new ErrorFacetBuilder().isDefinedAt(event)).isFalse();
  }

  private static class TestException extends Exception {

    public TestException(String message) {
      super(message);
    }
  }
}
