/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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
import io.openlineage.spark.agent.util.ExtensionClassloader;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.extension.OpenLineageExtensionProvider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A helper class that uses reflection to call all methods of SparkOpenLineageExtensionVisitor,
 * which are exposed by the extensions implementing interfaces from `spark-extension-interfaces`
 * package.
 */
@Slf4j
public final class SparkOpenLineageExtensionVisitorWrapper {

  private static final String providerCanonicalName =
      "io.openlineage.spark.extension.OpenLineageExtensionProvider";

  private static final ByteBuffer providerClassBytes;

  static {
    try {
      providerClassBytes = getProviderClassBytes(Thread.currentThread().getContextClassLoader());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final ClassLoader currentThreadClassloader =
      Thread.currentThread().getContextClassLoader();

  /**
   * Stores instances of SparkOpenLineageExtensionVisitor provided by connectors that implement
   * interfaces from spark-extension-interfaces.
   */
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

  @SuppressWarnings("PMD") // always point locally
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
      throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
    List<Object> objects = new ArrayList<>();
    // The following sequence of operations must be preserved as is.
    // We cannot use ResourceFinder or ServiceLoader to determine if there are any
    // OpenLineageExtensionProvider implementations because doing so involves classloading
    // machinery.
    // As a result, there is no reliable way to bypass the entire mechanism, even if
    // no OpenLineageExtensionProvider implementations are present.

    List<ClassLoader> availableClassloaders =
        Thread.getAllStackTraces().keySet().stream()
            .map(Thread::getContextClassLoader)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // Mutates the state of available classloader(s)
    loadProviderToAvailableClassloaders(availableClassloaders);
    ExtensionClassloader classLoader = new ExtensionClassloader(availableClassloaders);

    ServiceLoader<OpenLineageExtensionProvider> serviceLoader =
        ServiceLoader.load(OpenLineageExtensionProvider.class, classLoader);

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

  private static void loadProviderToAvailableClassloaders(List<ClassLoader> classloaders)
      throws IOException {
    List<ClassLoader> filteredClassloaders =
        classloaders.stream()
            // Skip the class loader associated with SparkOpenLineageExtensionVisitorWrapper
            .filter(cl -> !(currentThreadClassloader.equals(cl)))
            // Skip class loaders that have already loaded OpenLineageExtensionProvider
            .filter(cl -> !hasLoadedProvider(cl))
            .collect(Collectors.toList());

    if (!filteredClassloaders.isEmpty()) {
      log.warn(
          "An illegal reflective access operation will occur when using the openlineage-spark integration with a "
              + "Spark connector that implements spark-openlineage extension interfaces. \n"
              + "This issue arises when the openlineage-spark integration and the Spark connector are loaded using "
              + "different classloaders. For example, if one library is loaded using the --jars parameter while the other "
              + "is placed in the /usr/lib/spark/jars directory. \n"
              + "In this case, the OpenLineageExtensionProvider class will only be accessible to the class loader that "
              + "loaded the openlineage-spark integration. If the other class loader attempts to access this class, it "
              + "will trigger an illegal reflective access operation.\n"
              + "To prevent this, ensure that both the openlineage-spark integration and the Spark connector are loaded "
              + "using the same class loader. This can be achieved by: \n"
              + "1. Placing both libraries in the /usr/lib/spark/jars directory, or \n"
              + "2. Loading both libraries through the --jars parameter.");
    }

    filteredClassloaders.forEach(
        cl -> {
          try {
            MethodUtils.invokeMethod(
                cl, true, "defineClass", providerCanonicalName, providerClassBytes, null);
            log.trace("{} succeeded to load a class", cl);
          } catch (InvocationTargetException ex) {
            if (!(ex.getCause() instanceof LinkageError)) {
              log.error("Failed to load OpenLineageExtensionProvider class", ex.getCause());
            }
          } catch (Exception e) {
            log.error("{}: Failed to load OpenLineageExtensionProvider class ", cl, e);
          }
        });
  }

  private static boolean hasLoadedProvider(ClassLoader classLoader) {
    try {
      classLoader.loadClass(providerCanonicalName);
      return true;
    } catch (Exception | Error e) {
      log.trace("{} classloader failed to load OpenLineageExtensionProvider class", classLoader, e);
      return false;
    }
  }

  private static ByteBuffer getProviderClassBytes(ClassLoader classLoader) throws IOException {
    String classPath =
        SparkOpenLineageExtensionVisitorWrapper.providerCanonicalName.replace('.', '/') + ".class";

    try (InputStream is = classLoader.getResourceAsStream(classPath)) {
      if (is == null) {
        throw new IOException(
            "Class not found: "
                + SparkOpenLineageExtensionVisitorWrapper.providerCanonicalName
                + " using classloader: "
                + classLoader);
      }

      byte[] bytes = IOUtils.toByteArray(is);
      return ByteBuffer.wrap(bytes);
    }
  }

  private static Object getClassInstance(String className)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> loadedClass = Class.forName(className);
    Object classInstance = loadedClass.newInstance();
    return classInstance;
  }

  @SuppressWarnings("PMD") // always point locally
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

  @SuppressWarnings("PMD") // always point locally
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
