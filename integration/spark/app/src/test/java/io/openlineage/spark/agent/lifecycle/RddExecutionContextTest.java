/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link RddExecutionContext} applies {@code
 * spark.openlineage.dataset.removePath.pattern} to inputs/outputs of RDD jobs, same as {@link
 * OpenLineageRunEventBuilder} already does for non-RDD jobs. See
 * https://github.com/OpenLineage/OpenLineage/issues/4719
 */
class RddExecutionContextTest {

  private static final String REMOVE_PATH_PATTERN = "spark.openlineage.dataset.removePath.pattern";

  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private final OpenLineageRunEventBuilder runEventBuilder =
      new OpenLineageRunEventBuilder(olContext, mock(OpenLineageEventHandlerFactory.class));
  private final RddExecutionContext context =
      new RddExecutionContext(olContext, eventEmitter, runEventBuilder);

  private SparkConf sparkConf;

  @BeforeEach
  void setup() {
    sparkConf = mock(SparkConf.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(olContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
  }

  @Test
  void buildInputsRemovesConfiguredPathPatternForRddJobs() {
    when(sparkConf.contains(REMOVE_PATH_PATTERN)).thenReturn(true);
    when(sparkConf.get(REMOVE_PATH_PATTERN)).thenReturn("(?<remove>tmp)");

    List<DatasetIdentifier> inputs =
        Collections.singletonList(new DatasetIdentifier("/tmp/input.csv", "file"));

    List<InputDataset> result = context.buildInputs(inputs, false);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo("//input.csv");
  }

  @Test
  void buildOutputsRemovesConfiguredPathPatternForRddJobs() {
    when(sparkConf.contains(REMOVE_PATH_PATTERN)).thenReturn(true);
    when(sparkConf.get(REMOVE_PATH_PATTERN)).thenReturn("(?<remove>tmp)");

    List<URI> outputs = Collections.singletonList(URI.create("file:///tmp/output.csv"));

    List<OutputDataset> result = context.buildOutputs(outputs, false);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo("//output.csv");
  }

  @Test
  void buildInputsLeavesNameUnchangedWhenPatternNotConfigured() {
    when(sparkConf.contains(REMOVE_PATH_PATTERN)).thenReturn(false);

    List<DatasetIdentifier> inputs =
        Collections.singletonList(new DatasetIdentifier("/tmp/input.csv", "file"));

    List<InputDataset> result = context.buildInputs(inputs, false);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo("/tmp/input.csv");
  }
}
