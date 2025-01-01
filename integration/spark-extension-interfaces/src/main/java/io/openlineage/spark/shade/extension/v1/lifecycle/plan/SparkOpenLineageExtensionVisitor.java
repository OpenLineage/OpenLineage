/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.shade.extension.v1.lifecycle.plan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.shade.extension.v1.InputDatasetWithDelegate;
import io.openlineage.spark.shade.extension.v1.InputDatasetWithFacets;
import io.openlineage.spark.shade.extension.v1.InputDatasetWithIdentifier;
import io.openlineage.spark.shade.extension.v1.InputLineageNode;
import io.openlineage.spark.shade.extension.v1.LineageRelation;
import io.openlineage.spark.shade.extension.v1.LineageRelationProvider;
import io.openlineage.spark.shade.extension.v1.OutputDatasetWithDelegate;
import io.openlineage.spark.shade.extension.v1.OutputDatasetWithFacets;
import io.openlineage.spark.shade.extension.v1.OutputDatasetWithIdentifier;
import io.openlineage.spark.shade.extension.v1.OutputLineageNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * This class serves as a visitor that wraps method calls for handling input and output lineage in
 * Spark jobs, as defined in the OpenLineage-Spark extension.
 *
 * <p>The OpenLineage-Spark library uses reflection to access these wrapper methods for extracting
 * lineage information from Spark's LogicalPlan and other relevant components. The visitor class
 * handles different types of lineage nodes, such as {@link InputLineageNode} and {@link
 * OutputLineageNode}, and allows conversion to a format suitable for lineage tracking.
 */
public final class SparkOpenLineageExtensionVisitor {
  private static final ObjectMapper mapper = OpenLineageClientUtils.newObjectMapper();
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  /**
   * Determines if the given {@code lineageNode} is of a type that this visitor can process.
   * Specifically, it checks if the object is an instance of {@link LineageRelationProvider}, {@link
   * LineageRelation}, {@link InputLineageNode}, or {@link OutputLineageNode}.
   *
   * @param lineageNode the node representing a lineage component
   * @return {@code true} if the node is of a supported type, {@code false} otherwise
   */
  public boolean isDefinedAt(Object lineageNode) {
    return lineageNode instanceof LineageRelationProvider
        || lineageNode instanceof LineageRelation
        || lineageNode instanceof InputLineageNode
        || lineageNode instanceof OutputLineageNode;
  }

  /**
   * Applies the visitor to a {@link LineageRelationProvider}, extracting lineage information such
   * as the {@link DatasetIdentifier} from the provided {@code lineageNode}.
   *
   * @param lineageNode the lineage node to process
   * @param sparkListenerEventName the name of the Spark listener event
   * @param sqlContext the SQL context of the current Spark execution
   * @param parameters additional parameters relevant to the lineage extraction
   * @return a map containing lineage information in a serialized format
   */
  public Map<String, Object> apply(
      Object lineageNode, String sparkListenerEventName, Object sqlContext, Object parameters) {
    if (lineageNode instanceof LineageRelationProvider) {
      LineageRelationProvider provider = (LineageRelationProvider) lineageNode;
      DatasetIdentifier datasetIdentifier =
          provider.getLineageDatasetIdentifier(
              sparkListenerEventName, openLineage, sqlContext, parameters);
      return mapper.convertValue(datasetIdentifier, new TypeReference<Map<String, Object>>() {});
    }
    return Collections.emptyMap();
  }

  /**
   * Applies the visitor to a {@link LineageRelation}, {@link InputLineageNode}, or {@link
   * OutputLineageNode}, extracting and serializing the relevant lineage information.
   *
   * @param lineageNode the lineage node to process
   * @param sparkListenerEventName the name of the Spark listener event
   * @return a map containing the serialized lineage data
   */
  public Map<String, Object> apply(Object lineageNode, String sparkListenerEventName) {
    if (lineageNode instanceof LineageRelation) {
      LineageRelation lineageRelation = (LineageRelation) lineageNode;
      DatasetIdentifier datasetIdentifier =
          lineageRelation.getLineageDatasetIdentifier(sparkListenerEventName, openLineage);
      return mapper.convertValue(datasetIdentifier, new TypeReference<Map<String, Object>>() {});
    } else if (lineageNode instanceof InputLineageNode) {
      InputLineageNode inputLineageNode = (InputLineageNode) lineageNode;
      return handleInputLineageNode(sparkListenerEventName, inputLineageNode);
    } else if (lineageNode instanceof OutputLineageNode) {
      OutputLineageNode outputLineageNode = (OutputLineageNode) lineageNode;
      return handleOutputLineageNode(sparkListenerEventName, outputLineageNode);
    }
    return Collections.emptyMap();
  }

  private Map<String, Object> handleInputLineageNode(
      String sparkListenerEventName, InputLineageNode inputLineageNode) {
    List<InputDatasetWithFacets> inputs =
        inputLineageNode.getInputs(sparkListenerEventName, openLineage);
    List<InputDataset> inputDatasets =
        inputs.stream()
            .filter(d -> d instanceof InputDatasetWithIdentifier)
            .map(d -> (InputDatasetWithIdentifier) d)
            .map(
                d ->
                    openLineage
                        .newInputDatasetBuilder()
                        .namespace(d.getDatasetIdentifier().getNamespace())
                        .name(d.getDatasetIdentifier().getName())
                        .facets(d.getDatasetFacetsBuilder().build())
                        .inputFacets(d.getInputFacetsBuilder().build())
                        .build())
            .collect(Collectors.toList());

    List<Object> delegateNodes =
        inputs.stream()
            .filter(d -> d instanceof InputDatasetWithDelegate)
            .map(d -> (InputDatasetWithDelegate) d)
            .map(InputDatasetWithDelegate::getNode)
            .collect(Collectors.toList());
    List<Map<String, Object>> serializedDatasets =
        mapper.convertValue(inputDatasets, new TypeReference<List<Map<String, Object>>>() {});
    return buildMapWithDatasetsAndDelegates(serializedDatasets, delegateNodes);
  }

  private Map<String, Object> handleOutputLineageNode(
      String sparkListenerEventName, OutputLineageNode outputLineageNode) {
    List<OutputDatasetWithFacets> outputs =
        outputLineageNode.getOutputs(sparkListenerEventName, openLineage);

    List<OutputDataset> outputDatasets =
        outputs.stream()
            .filter(d -> d instanceof OutputDatasetWithIdentifier)
            .map(d -> (OutputDatasetWithIdentifier) d)
            .map(
                d ->
                    openLineage
                        .newOutputDatasetBuilder()
                        .namespace(d.getDatasetIdentifier().getNamespace())
                        .name(d.getDatasetIdentifier().getName())
                        .facets(d.getDatasetFacetsBuilder().build())
                        .outputFacets(d.getOutputFacetsBuilder().build())
                        .build())
            .collect(Collectors.toList());

    List<Object> delegateNodes =
        outputs.stream()
            .filter(d -> d instanceof OutputDatasetWithDelegate)
            .map(d -> (OutputDatasetWithDelegate) d)
            .map(OutputDatasetWithDelegate::getNode)
            .collect(Collectors.toList());
    List<Map<String, Object>> serializedDatasets =
        mapper.convertValue(outputDatasets, new TypeReference<List<Map<String, Object>>>() {});
    return buildMapWithDatasetsAndDelegates(serializedDatasets, delegateNodes);
  }

  private static Map<String, Object> buildMapWithDatasetsAndDelegates(
      List<Map<String, Object>> serializedDatasets, List<Object> delegateNodes) {
    Map<String, Object> map = new HashMap<>();
    map.put("datasets", serializedDatasets);
    map.put("delegateNodes", delegateNodes);
    return map;
  }

  /** Utility class to handle versioning for the OpenLineage producer URI. */
  private static class Versions {
    public static final URI OPEN_LINEAGE_PRODUCER_URI = getProducerUri();

    private static URI getProducerUri() {
      return URI.create(
          String.format(
              "https://github.com/OpenLineage/OpenLineage/tree/%s/integration/spark-extension-interfaces",
              getVersion()));
    }

    /**
     * Retrieves the version information from a properties file.
     *
     * @return the version string, or "main" if the version cannot be determined
     */
    @SuppressWarnings("PMD")
    public static String getVersion() {
      try {
        Properties properties = new Properties();
        InputStream is = Versions.class.getResourceAsStream("version.properties");
        properties.load(is);
        return properties.getProperty("version");
      } catch (IOException exception) {
        return "main";
      }
    }
  }
}
