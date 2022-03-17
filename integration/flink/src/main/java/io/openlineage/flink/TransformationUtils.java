package io.openlineage.flink;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

/**
 * Transform list-of-connected-dags-like structure to list of sources and sinks TODO: have to
 * preserve DAG information for precise source-to-sink dataset mappings
 */
@Slf4j
public class TransformationUtils {

  private final List<Object> sources = new ArrayList<>();
  private final List<Object> sinks = new ArrayList<>();

  private final Set<String> processedTransformations = new HashSet<>();

  public List<SinkLineage> convertToVisitable(List<Transformation<?>> transformations) {
    List<SinkLineage> lineages = new ArrayList<>();
    for (Transformation<?> transformation : transformations) {
      var sinkLineage = processSink(transformation);
      sinkLineage.ifPresent(lineages::add);
    }
    return lineages;
  }

  public Optional<SinkLineage> processSink(Transformation<?> transformation) {
    List<Object> sources = new ArrayList<>();
    Object sink;
    if (transformation instanceof SinkTransformation) {
      sink = processSinkTransformation(transformation);
    } else if (transformation instanceof LegacySinkTransformation) {
      sink = processLegacySinkTransformation(transformation);
    } else {
      return Optional.empty();
    }

    // Java streams do not like the unchecked generic casts we're doing here.
    // So, with regular for we go.
    List<Transformation<?>> predecessors = transformation.getTransitivePredecessors();
    for (var predecessor : predecessors) {
      var source = processSource(predecessor);
      source.ifPresent(sources::add);
    }
    return Optional.of(new SinkLineage(sources, sink));
  }

  public Optional<Object> processSource(Transformation<?> transformation) {
    if (transformation instanceof SourceTransformation) {
      return Optional.of(processSourceTransformation(transformation));
    } else if (transformation instanceof LegacySourceTransformation) {
      return Optional.of(processLegacySourceTransformation(transformation));
    }
    return Optional.empty();
  }

  public Object processSourceTransformation(Transformation<?> genericTransformation) {
    SourceTransformation transformation = (SourceTransformation) genericTransformation;
    return transformation.getSource();
  }

  public Object processLegacySourceTransformation(Transformation<?> genericTransformation) {
    LegacySourceTransformation transformation = (LegacySourceTransformation) genericTransformation;
    return transformation.getOperator().getUserFunction();
  }

  public Object processSinkTransformation(Transformation<?> genericTransformation) {
    SinkTransformation transformation = (SinkTransformation) genericTransformation;
    return transformation.getSink();
  }

  public Object processLegacySinkTransformation(Transformation<?> genericTransformation) {
    LegacySinkTransformation transformation = (LegacySinkTransformation) genericTransformation;
    return transformation.getOperator().getUserFunction();
  }
}
