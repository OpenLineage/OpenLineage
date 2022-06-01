/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.shared.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.shared.api.AbstractInputDatasetBuilder;
import io.openlineage.spark.shared.api.OpenLineageContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Build a collection of {@link InputDataset}s from a {@link
 * HadoopRDD}
 */
@Slf4j
public class HadoopRDDInputDatasetBuilder extends AbstractInputDatasetBuilder<RDD<?>> {

  public HadoopRDDInputDatasetBuilder(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(RDD<?> x) {
    return x instanceof HadoopRDD || x instanceof NewHadoopRDD;
  }

  @Override
  public Collection<InputDataset> apply(RDD<?> x) {
    return findInputs(x).stream().map(this::buildInputDataset).collect(Collectors.toList());
  }

  protected InputDataset buildInputDataset(URI uri) {
    DatasetParser.DatasetParseResult result = DatasetParser.parse(uri);
    return new OpenLineage.InputDatasetBuilder()
        .name(result.getName())
        .namespace(result.getNamespace())
        .build();
  }

  protected List<URI> findInputs(RDD<?> rdd) {
    List<URI> result = new ArrayList<>();
    Path[] inputPaths = getInputPaths(rdd);
    if (inputPaths != null) {
      for (Path path : inputPaths) {
        result.add(getDatasetUri(path.toUri()));
      }
    }
    return result;
  }

  protected Path[] getInputPaths(RDD<?> rdd) {
    Path[] inputPaths = null;
    if (rdd instanceof HadoopRDD) {
      inputPaths =
          org.apache.hadoop.mapred.FileInputFormat.getInputPaths(
              ((HadoopRDD<?, ?>) rdd).getJobConf());
    } else if (rdd instanceof NewHadoopRDD) {
      try {
        inputPaths =
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(
                new Job(((NewHadoopRDD<?, ?>) rdd).getConf()));
      } catch (IOException e) {
        log.error("Openlineage spark agent could not get input paths", e);
      }
    }
    return inputPaths;
  }

  // exposed for testing
  protected URI getDatasetUri(URI pathUri) {
    return pathUri;
  }
}
