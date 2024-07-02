/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

/**
 * A helper class that uses reflection to call all methods of SparkOpenLineageExtensionVisitor,
 * which are exposed by the extensions implementing interfaces from `spark-interfaces-shaded`
 * package.
 */
@Slf4j
public final class SparkOpenLineageExtensionVisitorWrapper {
  private final List<Object> extensionObjects;
  private final boolean hasLoadedObjects;
  private final ObjectMapper objectMapper =
      OpenLineageClientUtils.newObjectMapper()
          .addMixIn(DatasetIdentifier.class, DatasetIdentifierMixin.class)
          .addMixIn(Symlink.class, SymlinkMixin.class);

  public SparkOpenLineageExtensionVisitorWrapper(SparkOpenLineageConfig config) {
    try {
      extensionObjects = init(config.getTestExtensionProvider());
      this.hasLoadedObjects = !extensionObjects.isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isDefinedAt(Object object) {
    return hasLoadedObjects
        && extensionObjects.stream()
            .map(o -> getMethod(o, "isDefinedAt", Object.class))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .anyMatch(
                objectAndMethod -> {
                  try {
                    return (boolean) objectAndMethod.right.invoke(objectAndMethod.left, object);
                  } catch (Exception e) {
                    log.error(
                        "Can't invoke 'isDefinedAt' method on {} class instance",
                        objectAndMethod.left.getClass().getCanonicalName());
                  }
                  return false;
                });
  }

  public DatasetIdentifier getLineageDatasetIdentifier(
      Object lineageNode, String sparkListenerEventName, Object sqlContext, Object parameters) {
    if (!hasLoadedObjects) {
      return null;
    } else {
      final List<ImmutablePair<Object, Method>> methodsToCall =
          extensionObjects.stream()
              .map(
                  o ->
                      getMethod(o, "apply", Object.class, String.class, Object.class, Object.class))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());

      for (ImmutablePair<Object, Method> objectAndMethod : methodsToCall) {
        try {
          Map<String, Object> result =
              (Map<String, Object>)
                  objectAndMethod.right.invoke(
                      objectAndMethod.left,
                      lineageNode,
                      sparkListenerEventName,
                      sqlContext,
                      parameters);
          if (result != null && !result.isEmpty()) {
            return objectMapper.convertValue(result, DatasetIdentifier.class);
          }
        } catch (Exception e) {
          log.warn(
              "Can't invoke apply method on {} class instance",
              objectAndMethod.left.getClass().getCanonicalName());
        }
      }
    }
    return null;
  }

  public DatasetIdentifier getLineageDatasetIdentifier(
      Object lineageNode, String sparkListenerEventName) {
    Map<String, Object> datasetIdentifier = callApply(lineageNode, sparkListenerEventName);
    return objectMapper.convertValue(datasetIdentifier, DatasetIdentifier.class);
  }

  @SuppressWarnings("unchecked")
  public Pair<List<InputDataset>, List<Object>> getInputs(
      Object lineageNode, String sparkListenerEventName) {
    Map<String, Object> inputs = callApply(lineageNode, sparkListenerEventName);

    List<Map<String, Object>> datasets = (List<Map<String, Object>>) inputs.get("datasets");
    List<Object> delagateNodes = (List<Object>) inputs.get("delegateNodes");
    return ImmutablePair.of(
        objectMapper.convertValue(datasets, new TypeReference<List<InputDataset>>() {}),
        delagateNodes);
  }

  @SuppressWarnings("unchecked")
  public Pair<List<OpenLineage.OutputDataset>, List<Object>> getOutputs(
      Object lineageNode, String sparkListenerEventName) {
    Map<String, Object> outputs = callApply(lineageNode, sparkListenerEventName);
    List<Map<String, Object>> datasets = (List<Map<String, Object>>) outputs.get("datasets");
    List<Object> delagateNodes = (List<Object>) outputs.get("delegateNodes");
    return ImmutablePair.of(
        objectMapper.convertValue(
            datasets, new TypeReference<List<OpenLineage.OutputDataset>>() {}),
        delagateNodes);
  }

  private Map<String, Object> callApply(Object lineageNode, String sparkListenerEventName) {
    if (!hasLoadedObjects) {
      return Collections.emptyMap();
    } else {
      final List<ImmutablePair<Object, Method>> methodsToCall =
          extensionObjects.stream()
              .map(o -> getMethod(o, "apply", Object.class, String.class))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());

      for (ImmutablePair<Object, Method> objectAndMethod : methodsToCall) {
        try {
          Map<String, Object> result =
              (Map<String, Object>)
                  objectAndMethod.right.invoke(
                      objectAndMethod.left, lineageNode, sparkListenerEventName);
          if (result != null && !result.isEmpty()) {
            return result;
          }
        } catch (Exception e) {
          log.error(
              "Can't invoke apply method on {} class instance",
              objectAndMethod.left.getClass().getCanonicalName());
        }
      }
    }
    return Collections.emptyMap();
  }

  private Optional<ImmutablePair<Object, Method>> getMethod(
      Object classInstance, String methodName, Class<?>... parameterTypes) {
    try {
      Method method = classInstance.getClass().getMethod(methodName, parameterTypes);
      method.setAccessible(true);
      return Optional.of(ImmutablePair.of(classInstance, method));
    } catch (NoSuchMethodException e) {
      log.warn(
          "No '{}' method found on {} class instance",
          methodName,
          classInstance.getClass().getCanonicalName());
    }
    return Optional.empty();
  }

  private static List<Object> init(String testExtensionProvider)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    List<Object> objects = new ArrayList<>();
    ServiceLoader<OpenLineageExtensionProvider> serviceLoader =
        ServiceLoader.load(OpenLineageExtensionProvider.class);
    for (OpenLineageExtensionProvider service : serviceLoader) {
      String className = service.getVisitorClassName();
      if (testExtensionProvider == null) {
        final Object classInstance = getClassInstance(className);
        objects.add(classInstance);
      } else if (testExtensionProvider.equals(service.getClass().getCanonicalName())) {
        Object classInstance = getClassInstance(className);
        objects.add(classInstance);
        break;
      }
    }
    return objects;
  }

  @NotNull
  private static Object getClassInstance(String className)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> loadedClass = Class.forName(className);
    Object classInstance = loadedClass.newInstance();
    return classInstance;
  }

  private abstract static class DatasetIdentifierMixin {
    private final String name;
    private final String namespace;
    private final List<Symlink> symlinks;

    @JsonCreator
    public DatasetIdentifierMixin(
        @JsonProperty("name") String name,
        @JsonProperty("namespace") String namespace,
        @JsonProperty("symlinks") List<Symlink> symlinks) {
      this.name = name;
      this.namespace = namespace;
      this.symlinks = symlinks;
    }
  }

  private abstract static class SymlinkMixin {
    private final String name;
    private final String namespace;
    private final DatasetIdentifier.SymlinkType type;

    @JsonCreator
    private SymlinkMixin(
        @JsonProperty("name") String name,
        @JsonProperty("namespace") String namespace,
        @JsonProperty("type") DatasetIdentifier.SymlinkType type) {
      this.name = name;
      this.namespace = namespace;
      this.type = type;
    }
  }
}
