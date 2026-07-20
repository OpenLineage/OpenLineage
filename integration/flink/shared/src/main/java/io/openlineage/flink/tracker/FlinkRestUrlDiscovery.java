/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestOptions;

/**
 * Utility for discovering the actual Flink REST API URL at runtime.
 *
 * <p>In Flink Application mode with host network (Kubernetes), the actual bound REST port of
 * {@code MiniDispatcherRestEndpoint} may differ from the configured {@code rest.port}. Discovery
 * strategies (tried in order):
 *
 * <ol>
 *   <li>User override via OpenLineage config {@code restApiBaseUrl}
 *   <li>Reflect on {@code MiniDispatcherRestEndpoint} via {@code ClusterEntrypoint} captured in a
 *       JVM shutdown hook — precise, same-JVM Application mode path
 *   <li>Linux {@code /proc/self/net/tcp} + HTTP {@code /overview} validation
 *   <li>Fallback to configured {@code rest.address} + {@code rest.port}
 * </ol>
 */
@Slf4j
@SuppressWarnings({
  "PMD.AvoidAccessibilityAlteration",
  "PMD.AssignmentInOperand",
  "PMD.AvoidLiteralsInIfCondition",
  "PMD.AvoidUsingHardCodedIP"
})
public class FlinkRestUrlDiscovery {

  private static final String CLUSTER_ENTRYPOINT_CLASS =
      "org.apache.flink.runtime.entrypoint.ClusterEntrypoint";

  private static final int HTTP_OK = 200;
  private static final String FLINK_VERSION_FIELD = "\"flink-version\"";
  private static final String TASK_MANAGERS_FIELD = "\"taskmanagers\"";
  private static final String LOCALHOST = "localhost";
  private static final String LOOPBACK_HOST = "127.0.0.1";
  private static final String TCP_LISTEN_STATE = "0A";
  private static final int HTTP_TIMEOUT_MS = 1000;

  private FlinkRestUrlDiscovery() {}

  public static String resolveJobsApiUrl(ReadableConfig flinkConfig, String restApiBaseUrlOverride) {
    if (restApiBaseUrlOverride != null && !restApiBaseUrlOverride.isEmpty()) {
      String base = restApiBaseUrlOverride.replaceAll("/+$", "");
      log.info("Using configured OpenLineage restApiBaseUrl: {}", base);
      return base + "/jobs";
    }

    String configuredHost =
        Optional.ofNullable(flinkConfig.get(RestOptions.ADDRESS)).orElse("localhost");
    int configuredPort = flinkConfig.get(RestOptions.PORT);

    Optional<String> miniDispatcherUrl = discoverViaMiniDispatcherRestEndpoint(configuredHost);
    if (miniDispatcherUrl.isPresent()) {
      log.info(
          "Discovered Flink REST URL via MiniDispatcherRestEndpoint: {}", miniDispatcherUrl.get());
      return miniDispatcherUrl.get() + "/jobs";
    }

    Optional<String> procUrl = discoverViaLinuxProcNet(configuredHost);
    if (procUrl.isPresent()) {
      log.info("Discovered Flink REST URL via /proc/self/net/tcp: {}", procUrl.get());
      return procUrl.get() + "/jobs";
    }

    log.warn(
        "Could not auto-discover Flink REST port; falling back to configured http://{}:{}. "
            + "If the port is wrong, set openlineage.restApiBaseUrl in flink-conf.yaml.",
        configuredHost,
        configuredPort);
    return String.format("http://%s:%s/jobs", configuredHost, configuredPort);
  }

  static Optional<String> discoverViaMiniDispatcherRestEndpoint(String configuredHost) {
    Optional<Object> entrypoint = findClusterEntrypoint();
    if (!entrypoint.isPresent()) {
      log.debug("ClusterEntrypoint not found in JVM shutdown hooks");
      return Optional.empty();
    }

    try {
      Field clusterComponentField =
          findDeclaredField(entrypoint.get().getClass(), "clusterComponent");
      if (clusterComponentField == null) {
        log.debug("Field 'clusterComponent' not found on {}", entrypoint.get().getClass());
        return Optional.empty();
      }
      clusterComponentField.setAccessible(true);
      Object clusterComponent = clusterComponentField.get(entrypoint.get());
      if (clusterComponent == null) {
        log.debug("clusterComponent is null");
        return Optional.empty();
      }
      log.debug("Found clusterComponent: {}", clusterComponent.getClass().getName());

      Object restEndpoint = null;
      for (Class<?> cls = clusterComponent.getClass();
          cls != null && cls != Object.class;
          cls = cls.getSuperclass()) {
        for (Field f : cls.getDeclaredFields()) {
          f.setAccessible(true);
          Object val;
          try {
            val = f.get(clusterComponent);
          } catch (Exception e) {
            continue;
          }
          if (val == null) {
            continue;
          }
          String valClassName = val.getClass().getName();
          log.debug("  clusterComponent field {}={}", f.getName(), valClassName);
          if (valClassName.contains("RestEndpoint") || valClassName.contains("WebMonitor")) {
            restEndpoint = val;
            break;
          }
        }
        if (restEndpoint != null) {
          break;
        }
      }
      if (restEndpoint == null) {
        log.debug(
            "No REST endpoint field found in clusterComponent {}",
            clusterComponent.getClass().getName());
        return Optional.empty();
      }
      log.debug("Found restEndpoint: {}", restEndpoint.getClass().getName());

      Field serverAddressField = findDeclaredField(restEndpoint.getClass(), "serverAddress");
      if (serverAddressField != null) {
        serverAddressField.setAccessible(true);
        InetSocketAddress addr = (InetSocketAddress) serverAddressField.get(restEndpoint);
        if (addr != null && addr.getPort() > 0) {
          String host =
              addr.getAddress().isAnyLocalAddress() ? configuredHost : addr.getHostString();
          log.debug("serverAddress = {}:{}", addr.getHostString(), addr.getPort());
          return Optional.of(String.format("http://%s:%d", host, addr.getPort()));
        }
      }

      try {
        Method getRestBaseUrl = restEndpoint.getClass().getMethod("getRestBaseUrl");
        String baseUrl = (String) getRestBaseUrl.invoke(restEndpoint);
        if (baseUrl != null && !baseUrl.isEmpty()) {
          log.debug("getRestBaseUrl() = {}", baseUrl);
          return Optional.of(baseUrl);
        }
      } catch (Exception e) {
        log.debug("getRestBaseUrl() not available: {}", e.getMessage());
      }

    } catch (Exception e) {
      log.debug("Error reflecting on MiniDispatcherRestEndpoint: {}", e.getMessage());
    }
    return Optional.empty();
  }

  /**
   * Finds the {@code ClusterEntrypoint} instance from JVM shutdown hooks.
   *
   * <p>Flink registers a shutdown hook via {@code ShutdownHookUtil.addShutdownHook(this, ...)}
   * inside {@code ClusterEntrypoint}. The hook lambda captures {@code this} (the entrypoint
   * instance) as a synthetic field.
   */
  static Optional<Object> findClusterEntrypoint() {
    try {
      Class<?> appShutdownHooksClass = Class.forName("java.lang.ApplicationShutdownHooks");
      Field hooksField = appShutdownHooksClass.getDeclaredField("hooks");
      hooksField.setAccessible(true);

      @SuppressWarnings("unchecked")
      Map<Thread, Thread> hooks = (Map<Thread, Thread>) hooksField.get(null);
      if (hooks == null) {
        log.debug("ApplicationShutdownHooks.hooks is null");
        return Optional.empty();
      }

      log.debug("Scanning {} JVM shutdown hook threads for ClusterEntrypoint", hooks.size());
      for (Thread thread : new HashSet<>(hooks.keySet())) {
        log.debug("Shutdown hook thread: name={}", thread.getName());
        Optional<Object> entrypoint = tryExtractClusterEntrypoint(thread);
        if (entrypoint.isPresent()) {
          log.debug("Found ClusterEntrypoint: {}", entrypoint.get().getClass().getName());
          return entrypoint;
        }
      }
    } catch (Exception e) {
      log.debug("Error scanning JVM shutdown hooks: {}", e.getMessage());
    }
    return Optional.empty();
  }

  private static Optional<Object> tryExtractClusterEntrypoint(Thread thread) {
    try {
      Runnable target = getThreadTarget(thread);
      if (target == null) {
        log.debug("Thread {} has null target", thread.getName());
        return Optional.empty();
      }
      log.debug("Thread {} target class: {}", thread.getName(), target.getClass().getName());

      for (Field f : target.getClass().getDeclaredFields()) {
        f.setAccessible(true);
        Object val;
        try {
          val = f.get(target);
        } catch (Exception e) {
          continue;
        }
        if (val == null) {
          continue;
        }
        log.debug(
            "  Field {} = {} (type: {})",
            f.getName(),
            val.getClass().getSimpleName(),
            f.getType().getSimpleName());

        if (isClusterEntrypoint(val)) {
          return Optional.of(val);
        }

        if (val.getClass().getName().contains("ClusterEntrypoint")) {
          for (Field inner : val.getClass().getDeclaredFields()) {
            inner.setAccessible(true);
            Object innerVal;
            try {
              innerVal = inner.get(val);
            } catch (Exception e) {
              continue;
            }
            log.debug(
                "    Inner field {} = {} (type: {})",
                inner.getName(),
                innerVal == null ? "null" : innerVal.getClass().getSimpleName(),
                inner.getType().getSimpleName());
            if (innerVal != null && isClusterEntrypoint(innerVal)) {
              return Optional.of(innerVal);
            }
          }
        }
      }
    } catch (Exception e) {
      log.debug("Error inspecting thread {}: {}", thread.getName(), e.getMessage());
    }
    return Optional.empty();
  }

  private static Runnable getThreadTarget(Thread thread) {
    try {
      Field targetField = Thread.class.getDeclaredField("target");
      targetField.setAccessible(true);
      return (Runnable) targetField.get(thread);
    } catch (Exception e) {
      log.debug("Cannot access Thread.target: {}", e.getMessage());
      return null;
    }
  }

  private static boolean isClusterEntrypoint(Object obj) {
    Class<?> c = obj.getClass();
    while (c != null && c != Object.class) {
      if (CLUSTER_ENTRYPOINT_CLASS.equals(c.getName())) {
        return true;
      }
      c = c.getSuperclass();
    }
    return false;
  }

  static Optional<String> discoverViaLinuxProcNet(String configuredHost) {
    Set<Integer> ports = readListeningPortsFromProc();
    if (ports.isEmpty()) {
      log.debug("/proc/self/net/tcp not available or no listening ports found");
      return Optional.empty();
    }
    log.debug(
        "Checking {} ports from /proc/self/net/tcp for Flink REST endpoint on host {}",
        ports.size(),
        configuredHost);

    for (String host : Arrays.asList(configuredHost, LOOPBACK_HOST, LOCALHOST)) {
      for (int port : ports) {
        if (isFlinkRestEndpoint(host, port)) {
          return Optional.of(String.format("http://%s:%d", configuredHost, port));
        }
      }
    }
    return Optional.empty();
  }

  static Set<Integer> readListeningPortsFromProc() {
    Set<Integer> ports = new LinkedHashSet<>();
    File procTcp = new File("/proc/self/net/tcp");
    if (!procTcp.exists()) {
      return ports;
    }
    try (BufferedReader br = new BufferedReader(new FileReader(procTcp))) {
      br.readLine();
      String line = br.readLine();
      while (line != null) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length >= 4 && TCP_LISTEN_STATE.equalsIgnoreCase(parts[3])) {
          String localAddr = parts[1];
          int colon = localAddr.lastIndexOf(':');
          if (colon >= 0) {
            try {
              int port = Integer.parseInt(localAddr.substring(colon + 1), 16);
              if (port > 0 && port < 65536) {
                ports.add(port);
              }
            } catch (NumberFormatException ignored) {
            }
          }
        }
        line = br.readLine();
      }
    } catch (IOException e) {
      log.debug("Cannot read /proc/self/net/tcp: {}", e.getMessage());
    }
    return ports;
  }

  static boolean isFlinkRestEndpoint(String host, int port) {
    try {
      URL url = new URL("http", host, port, "/overview");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(HTTP_TIMEOUT_MS);
      conn.setReadTimeout(HTTP_TIMEOUT_MS);
      conn.setRequestMethod("GET");
      conn.setInstanceFollowRedirects(false);
      if (conn.getResponseCode() == HTTP_OK) {
        try (InputStream is = conn.getInputStream()) {
          byte[] buf = new byte[4096];
          int read = is.read(buf);
          if (read > 0) {
            String body = new String(buf, 0, read, StandardCharsets.UTF_8);
            boolean match =
                body.contains(FLINK_VERSION_FIELD) || body.contains(TASK_MANAGERS_FIELD);
            if (match) {
              log.debug("Confirmed Flink REST endpoint at {}:{}", host, port);
            }
            return match;
          }
        }
      }
    } catch (Exception ignored) {
    }
    return false;
  }

  private static Field findDeclaredField(Class<?> clazz, String name) {
    Class<?> c = clazz;
    while (c != null && c != Object.class) {
      try {
        return c.getDeclaredField(name);
      } catch (NoSuchFieldException e) {
        c = c.getSuperclass();
      }
    }
    return null;
  }
}
