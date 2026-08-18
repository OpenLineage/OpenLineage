/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage.JobEvent;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Translates between explicit lineage facets and legacy inputs, outputs and column lineage. */
final class LineageCompatibilityConverter {
  private static final String DATASET = "DATASET";
  private static final String LINEAGE_SCHEMA_URL =
      "https://openlineage.io/spec/facets/1-0-0/LineageFacet.json#/$defs/LineageJobFacet";
  private static final String COLUMN_LINEAGE_SCHEMA_URL =
      "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet";
  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();

  private LineageCompatibilityConverter() {}

  static RunEvent convert(RunEvent event, LineageCompatibility compatibility) {
    return convert(event, compatibility, RunEvent.class);
  }

  static JobEvent convert(JobEvent event, LineageCompatibility compatibility) {
    return convert(event, compatibility, JobEvent.class);
  }

  private static <T> T convert(
      T event, LineageCompatibility configuredCompatibility, Class<T> eventClass) {
    LineageCompatibility compatibility =
        configuredCompatibility == null ? LineageCompatibility.NONE : configuredCompatibility;
    if (compatibility == LineageCompatibility.NONE) {
      return event;
    }

    ObjectNode eventNode = MAPPER.valueToTree(event);
    ObjectNode facets = getJobFacets(eventNode, false);
    boolean hasLineage = facets != null && hasValue(facets, "lineage");
    boolean changed;
    switch (compatibility) {
      case LEGACY:
        changed = hasLineage && addLegacyLineage(eventNode, facets.get("lineage"));
        break;
      case MODERN:
        changed = !hasLineage && addModernLineage(eventNode);
        break;
      case BOTH:
        changed =
            hasLineage
                ? addLegacyLineage(eventNode, facets.get("lineage"))
                : addModernLineage(eventNode);
        break;
      default:
        changed = false;
        break;
    }

    if (!changed) {
      return event;
    }
    try {
      return MAPPER.treeToValue(eventNode, eventClass);
    } catch (JsonProcessingException e) {
      throw new OpenLineageClientException("Unable to translate lineage compatibility fields", e);
    }
  }

  /** Adds inputs, outputs and representable column lineage from a LineageJobFacet. */
  private static boolean addLegacyLineage(ObjectNode event, JsonNode lineage) {
    JsonNode entriesNode = lineage.get("entries");
    if (!entriesNodeIsUsable(entriesNode)) {
      return false;
    }

    ArrayNode inputs = arrayOrEmpty(event.get("inputs"));
    ArrayNode outputs = arrayOrEmpty(event.get("outputs"));
    Map<DatasetIdentifier, JsonNode> knownInputs = indexDatasets(inputs);
    Map<DatasetIdentifier, List<ObjectNode>> knownOutputs = indexObjectDatasets(outputs);
    Map<DatasetIdentifier, ObjectNode> generatedColumnLineage = new LinkedHashMap<>();
    boolean changed = false;

    for (JsonNode entryNode : entriesNode) {
      if (!entryNode.isObject()) {
        continue;
      }
      ObjectNode entry = (ObjectNode) entryNode;
      DatasetIdentifier target = isDataset(entry) ? DatasetIdentifier.from(entry) : null;
      if (target != null && !knownOutputs.containsKey(target)) {
        ObjectNode output = datasetIdentity(entry);
        outputs.add(output);
        knownOutputs.computeIfAbsent(target, unused -> new ArrayList<>()).add(output);
        changed = true;
      }

      changed |=
          collectLegacyInputsAndDatasetLineage(
              entry.get("inputs"), target, lineage, inputs, knownInputs, generatedColumnLineage);
      if (target != null) {
        changed |=
            collectLegacyFieldLineage(
                entry.get("fields"), target, lineage, inputs, knownInputs, generatedColumnLineage);
      }
    }

    if (inputs.size() > 0 && !inputs.equals(event.get("inputs"))) {
      event.set("inputs", inputs);
    }
    if (outputs.size() > 0 && !outputs.equals(event.get("outputs"))) {
      event.set("outputs", outputs);
    }

    for (Map.Entry<DatasetIdentifier, ObjectNode> generated : generatedColumnLineage.entrySet()) {
      List<ObjectNode> targetOutputs = knownOutputs.get(generated.getKey());
      if (targetOutputs != null) {
        changed |= mergeColumnLineage(targetOutputs, generated.getValue());
      }
    }
    return changed;
  }

  private static boolean entriesNodeIsUsable(JsonNode entries) {
    return entries != null && entries.isArray() && entries.size() > 0;
  }

  private static boolean collectLegacyInputsAndDatasetLineage(
      JsonNode lineageInputs,
      DatasetIdentifier target,
      JsonNode lineage,
      ArrayNode eventInputs,
      Map<DatasetIdentifier, JsonNode> knownInputs,
      Map<DatasetIdentifier, ObjectNode> generatedColumnLineage) {
    if (lineageInputs == null || !lineageInputs.isArray()) {
      return false;
    }
    boolean changed = false;
    for (JsonNode inputNode : lineageInputs) {
      if (!inputNode.isObject() || !isDataset(inputNode)) {
        continue;
      }
      ObjectNode input = (ObjectNode) inputNode;
      changed |= addLegacyInput(input, eventInputs, knownInputs);
      if (target != null && hasValue(input, "field")) {
        ObjectNode columnLineage =
            generatedColumnLineage.computeIfAbsent(
                target, unused -> newColumnLineageFacet(lineage));
        changed |= appendUnique(array(columnLineage, "dataset"), toLegacyInputField(input));
      }
    }
    return changed;
  }

  private static boolean collectLegacyFieldLineage(
      JsonNode fieldsNode,
      DatasetIdentifier target,
      JsonNode lineage,
      ArrayNode eventInputs,
      Map<DatasetIdentifier, JsonNode> knownInputs,
      Map<DatasetIdentifier, ObjectNode> generatedColumnLineage) {
    if (fieldsNode == null || !fieldsNode.isObject()) {
      return false;
    }
    boolean changed = false;
    Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode lineageInputs = field.getValue().get("inputs");
      if (lineageInputs == null || !lineageInputs.isArray()) {
        continue;
      }
      for (JsonNode inputNode : lineageInputs) {
        if (!inputNode.isObject() || !isDataset(inputNode)) {
          continue;
        }
        ObjectNode input = (ObjectNode) inputNode;
        changed |= addLegacyInput(input, eventInputs, knownInputs);
        if (hasValue(input, "field")) {
          ObjectNode columnLineage =
              generatedColumnLineage.computeIfAbsent(
                  target, unused -> newColumnLineageFacet(lineage));
          ObjectNode legacyField = object(object(columnLineage, "fields"), field.getKey());
          changed |= appendUnique(array(legacyField, "inputFields"), toLegacyInputField(input));
        }
      }
    }
    return changed;
  }

  private static boolean addLegacyInput(
      ObjectNode input, ArrayNode eventInputs, Map<DatasetIdentifier, JsonNode> knownInputs) {
    DatasetIdentifier identifier = DatasetIdentifier.from(input);
    if (identifier == null || knownInputs.containsKey(identifier)) {
      return false;
    }
    ObjectNode legacyInput = datasetIdentity(input);
    eventInputs.add(legacyInput);
    knownInputs.put(identifier, legacyInput);
    return true;
  }

  private static ObjectNode toLegacyInputField(ObjectNode input) {
    ObjectNode legacyInput = datasetIdentity(input);
    copyIfPresent(input, legacyInput, "field");
    copyIfPresent(input, legacyInput, "transformations");
    return legacyInput;
  }

  private static ObjectNode newColumnLineageFacet(JsonNode lineage) {
    ObjectNode columnLineage = MAPPER.createObjectNode();
    copyIfPresent(lineage, columnLineage, "_producer");
    columnLineage.put("_schemaURL", COLUMN_LINEAGE_SCHEMA_URL);
    columnLineage.set("fields", MAPPER.createObjectNode());
    return columnLineage;
  }

  /** Adds a LineageJobFacet from legacy inputs, outputs and column lineage. */
  private static boolean addModernLineage(ObjectNode event) {
    JsonNode outputsNode = event.get("outputs");
    if (outputsNode == null || !outputsNode.isArray() || outputsNode.size() == 0) {
      return false;
    }

    ArrayNode legacyInputs = arrayOrEmpty(event.get("inputs"));
    ArrayNode commonInputs = MAPPER.createArrayNode();
    for (JsonNode input : legacyInputs) {
      if (input.isObject()) {
        appendUnique(commonInputs, toModernDatasetInput((ObjectNode) input));
      }
    }

    Map<DatasetIdentifier, ObjectNode> entriesByTarget = new LinkedHashMap<>();
    for (JsonNode outputNode : outputsNode) {
      if (!outputNode.isObject()) {
        continue;
      }
      ObjectNode output = (ObjectNode) outputNode;
      DatasetIdentifier target = DatasetIdentifier.from(output);
      if (target == null) {
        continue;
      }
      ObjectNode entry = entriesByTarget.computeIfAbsent(target, unused -> newLineageEntry(output));
      ArrayNode entryInputs = array(entry, "inputs");
      appendUnique(entryInputs, commonInputs);
      addModernColumnLineage(entry, getColumnLineage(output));
    }

    if (entriesByTarget.isEmpty()) {
      return false;
    }
    ObjectNode lineage = MAPPER.createObjectNode();
    copyIfPresent(event, lineage, "producer", "_producer");
    lineage.put("_schemaURL", LINEAGE_SCHEMA_URL);
    ArrayNode entries = MAPPER.createArrayNode();
    entriesByTarget.values().forEach(entries::add);
    lineage.set("entries", entries);
    ObjectNode jobFacets = getJobFacets(event, true);
    if (jobFacets == null) {
      return false;
    }
    jobFacets.set("lineage", lineage);
    return true;
  }

  private static ObjectNode newLineageEntry(ObjectNode output) {
    ObjectNode entry = datasetIdentity(output);
    entry.put("type", DATASET);
    entry.set("inputs", MAPPER.createArrayNode());
    return entry;
  }

  private static JsonNode getColumnLineage(ObjectNode output) {
    JsonNode facets = output.get("facets");
    if (facets == null || !facets.isObject()) {
      return null;
    }
    JsonNode columnLineage = facets.get("columnLineage");
    return columnLineage != null && columnLineage.isObject() ? columnLineage : null;
  }

  private static void addModernColumnLineage(ObjectNode entry, JsonNode columnLineage) {
    if (columnLineage == null) {
      return;
    }
    JsonNode datasetInputs = columnLineage.get("dataset");
    if (datasetInputs != null && datasetInputs.isArray()) {
      ArrayNode entryInputs = array(entry, "inputs");
      for (JsonNode input : datasetInputs) {
        if (input.isObject()) {
          appendUnique(entryInputs, toModernInputField((ObjectNode) input));
        }
      }
    }

    JsonNode fieldsNode = columnLineage.get("fields");
    if (fieldsNode == null || !fieldsNode.isObject()) {
      return;
    }
    Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      JsonNode inputFields = field.getValue().get("inputFields");
      if (inputFields == null || !inputFields.isArray()) {
        continue;
      }
      ObjectNode modernField = object(object(entry, "fields"), field.getKey());
      ArrayNode modernInputs = array(modernField, "inputs");
      for (JsonNode input : inputFields) {
        if (input.isObject()) {
          appendUnique(modernInputs, toModernInputField((ObjectNode) input));
        }
      }
    }
  }

  private static ObjectNode toModernDatasetInput(ObjectNode input) {
    ObjectNode modernInput = datasetIdentity(input);
    modernInput.put("type", DATASET);
    return modernInput;
  }

  private static ObjectNode toModernInputField(ObjectNode input) {
    ObjectNode modernInput = toModernDatasetInput(input);
    copyIfPresent(input, modernInput, "field");
    copyIfPresent(input, modernInput, "transformations");
    return modernInput;
  }

  private static boolean mergeColumnLineage(List<ObjectNode> outputs, ObjectNode generated) {
    List<ObjectNode> existingFacets = new ArrayList<>();
    for (ObjectNode output : outputs) {
      JsonNode existing = getColumnLineage(output);
      if (existing != null) {
        existingFacets.add((ObjectNode) existing);
      }
    }
    if (existingFacets.isEmpty()) {
      object(outputs.get(0), "facets").set("columnLineage", generated);
      return true;
    }

    ObjectNode destination = existingFacets.get(0);
    boolean changed = false;
    JsonNode generatedDataset = generated.get("dataset");
    if (generatedDataset != null && generatedDataset.isArray()) {
      for (JsonNode input : generatedDataset) {
        if (!containsDatasetInput(existingFacets, input)) {
          array(destination, "dataset").add(input);
          changed = true;
        }
      }
    }

    Iterator<Map.Entry<String, JsonNode>> generatedFields = generated.get("fields").fields();
    while (generatedFields.hasNext()) {
      Map.Entry<String, JsonNode> field = generatedFields.next();
      JsonNode generatedInputs = field.getValue().get("inputFields");
      if (generatedInputs != null && generatedInputs.isArray()) {
        ObjectNode destinationField = findField(existingFacets, field.getKey());
        for (JsonNode input : generatedInputs) {
          if (!containsFieldInput(existingFacets, field.getKey(), input)) {
            if (destinationField == null) {
              destinationField = object(object(destination, "fields"), field.getKey());
            }
            array(destinationField, "inputFields").add(input);
            changed = true;
          }
        }
      }
    }
    return changed;
  }

  private static boolean containsDatasetInput(List<ObjectNode> facets, JsonNode input) {
    for (ObjectNode facet : facets) {
      JsonNode dataset = facet.get("dataset");
      if (dataset != null && dataset.isArray() && contains((ArrayNode) dataset, input)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsFieldInput(
      List<ObjectNode> facets, String fieldName, JsonNode input) {
    for (ObjectNode facet : facets) {
      ObjectNode field = findField(facet, fieldName);
      if (field != null) {
        JsonNode inputs = field.get("inputFields");
        if (inputs != null && inputs.isArray() && contains((ArrayNode) inputs, input)) {
          return true;
        }
      }
    }
    return false;
  }

  private static ObjectNode findField(List<ObjectNode> facets, String fieldName) {
    for (ObjectNode facet : facets) {
      ObjectNode field = findField(facet, fieldName);
      if (field != null) {
        return field;
      }
    }
    return null;
  }

  private static ObjectNode findField(ObjectNode facet, String fieldName) {
    JsonNode fields = facet.get("fields");
    if (fields == null || !fields.isObject()) {
      return null;
    }
    JsonNode field = fields.get(fieldName);
    return field != null && field.isObject() ? (ObjectNode) field : null;
  }

  private static ObjectNode getJobFacets(ObjectNode event, boolean create) {
    JsonNode jobNode = event.get("job");
    if (jobNode == null || !jobNode.isObject()) {
      return null;
    }
    JsonNode facetsNode = jobNode.get("facets");
    if (facetsNode != null && facetsNode.isObject()) {
      return (ObjectNode) facetsNode;
    }
    if (!create) {
      return null;
    }
    ObjectNode facets = MAPPER.createObjectNode();
    ((ObjectNode) jobNode).set("facets", facets);
    return facets;
  }

  private static Map<DatasetIdentifier, JsonNode> indexDatasets(ArrayNode datasets) {
    Map<DatasetIdentifier, JsonNode> indexed = new LinkedHashMap<>();
    for (JsonNode dataset : datasets) {
      DatasetIdentifier identifier = DatasetIdentifier.from(dataset);
      if (identifier != null) {
        indexed.putIfAbsent(identifier, dataset);
      }
    }
    return indexed;
  }

  private static Map<DatasetIdentifier, List<ObjectNode>> indexObjectDatasets(ArrayNode datasets) {
    Map<DatasetIdentifier, List<ObjectNode>> indexed = new LinkedHashMap<>();
    for (JsonNode dataset : datasets) {
      DatasetIdentifier identifier = DatasetIdentifier.from(dataset);
      if (identifier != null && dataset.isObject()) {
        indexed.computeIfAbsent(identifier, unused -> new ArrayList<>()).add((ObjectNode) dataset);
      }
    }
    return indexed;
  }

  private static ObjectNode datasetIdentity(JsonNode dataset) {
    ObjectNode identity = MAPPER.createObjectNode();
    copyIfPresent(dataset, identity, "namespace");
    copyIfPresent(dataset, identity, "name");
    return identity;
  }

  private static boolean isDataset(JsonNode node) {
    JsonNode type = node.get("type");
    return type != null && DATASET.equals(type.asText());
  }

  private static boolean hasValue(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value != null && !value.isNull();
  }

  private static ArrayNode arrayOrEmpty(JsonNode node) {
    return node != null && node.isArray() ? (ArrayNode) node : MAPPER.createArrayNode();
  }

  private static ArrayNode array(ObjectNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value != null && value.isArray()) {
      return (ArrayNode) value;
    }
    ArrayNode array = MAPPER.createArrayNode();
    parent.set(field, array);
    return array;
  }

  private static ObjectNode object(ObjectNode parent, String field) {
    JsonNode value = parent.get(field);
    if (value != null && value.isObject()) {
      return (ObjectNode) value;
    }
    ObjectNode object = MAPPER.createObjectNode();
    parent.set(field, object);
    return object;
  }

  private static boolean appendUnique(ArrayNode target, ArrayNode values) {
    boolean changed = false;
    for (JsonNode value : values) {
      changed |= appendUnique(target, value);
    }
    return changed;
  }

  private static boolean appendUnique(ArrayNode target, JsonNode value) {
    if (contains(target, value)) {
      return false;
    }
    target.add(value);
    return true;
  }

  private static boolean contains(ArrayNode values, JsonNode candidate) {
    for (JsonNode value : values) {
      if (value.equals(candidate)) {
        return true;
      }
    }
    return false;
  }

  private static void copyIfPresent(JsonNode source, ObjectNode target, String field) {
    copyIfPresent(source, target, field, field);
  }

  private static void copyIfPresent(
      JsonNode source, ObjectNode target, String sourceField, String targetField) {
    JsonNode value = source.get(sourceField);
    if (value != null && !value.isNull()) {
      target.set(targetField, value.deepCopy());
    }
  }

  private static final class DatasetIdentifier {
    private final String namespace;
    private final String name;

    private DatasetIdentifier(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    private static DatasetIdentifier from(JsonNode dataset) {
      JsonNode namespace = dataset.get("namespace");
      JsonNode name = dataset.get("name");
      if (namespace == null || namespace.isNull() || name == null || name.isNull()) {
        return null;
      }
      return new DatasetIdentifier(namespace.asText(), name.asText());
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof DatasetIdentifier)) {
        return false;
      }
      DatasetIdentifier that = (DatasetIdentifier) other;
      return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, name);
    }
  }
}
