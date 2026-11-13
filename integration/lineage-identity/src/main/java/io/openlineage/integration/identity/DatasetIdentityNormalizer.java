/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.integration.identity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.TransportConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Resolves table identities from a shared, Gravitino-verified source registry. */
public final class DatasetIdentityNormalizer {
  public static final String CONFIG_KEY = "openlineage.dataset.identity.sources";
  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();
  private final Map<String, Source> aliases = new HashMap<>();
  private final Map<String, Source> canonicalNamespaces = new HashMap<>();
  private final Set<Identity> managedIdentities = new HashSet<>();
  private final Set<Identity> managedCatalogs = new HashSet<>();
  private final GravitinoCatalogDiscovery discovery;

  private DatasetIdentityNormalizer(String config) throws IOException {
    discovery = null;
    JsonNode root = MAPPER.readTree(config);
    if (root == null || !root.isObject() || !root.path("sources").isArray()) {
      throw new IllegalArgumentException("Identity config must contain a sources array");
    }
    Set<String> sourceIds = new HashSet<>();
    for (JsonNode entry : root.path("sources")) {
      Source source = new Source(entry);
      if (!sourceIds.add(source.id))
        throw new IllegalArgumentException("Duplicate source id: " + source.id);
      if (canonicalNamespaces.putIfAbsent(source.namespace, source) != null) {
        throw new IllegalArgumentException(
            "External namespace belongs to multiple sources: " + source.namespace);
      }
      for (JsonNode alias : entry.path("aliases")) {
        String key = sourceKey(alias.asText());
        if (aliases.putIfAbsent(key, source) != null) {
          throw new IllegalArgumentException("Source alias belongs to multiple sources: " + key);
        }
      }
      for (Identity identity : source.managedTables.values()) {
        if (!managedIdentities.add(identity)) {
          throw new IllegalArgumentException(
              "Managed identity belongs to multiple source tables: " + identity.name);
        }
      }
      if (source.managedCatalog != null && !managedCatalogs.add(source.managedCatalog.identity)) {
        throw new IllegalArgumentException(
            "Managed catalog belongs to multiple sources: " + source.managedCatalog.identity.name);
      }
    }
    for (Identity identity : managedIdentities) {
      for (Identity catalog : managedCatalogs) {
        if (identity.namespace.equals(catalog.namespace)
            && identity.name.startsWith(catalog.name + ".")) {
          throw new IllegalArgumentException(
              "Managed identity conflicts with catalog: " + identity.name);
        }
      }
    }
  }

  private DatasetIdentityNormalizer(HttpConfig transport) {
    discovery = new GravitinoCatalogDiscovery(transport);
  }

  public static DatasetIdentityNormalizer fromTransport(TransportConfig transport) {
    if (!(transport instanceof HttpConfig)) return null;
    HttpConfig http = (HttpConfig) transport;
    URI url = http.getUrl();
    String path = url == null ? "" : url.getPath();
    if (!"/api/lineage".equals(http.getEndpoint())
        && (path == null || !path.endsWith("/api/lineage"))) return null;
    return new DatasetIdentityNormalizer(http);
  }

  public static DatasetIdentityNormalizer fromJson(String config) {
    if (config == null || config.trim().isEmpty()) return null;
    try {
      return new DatasetIdentityNormalizer(config);
    } catch (IOException error) {
      throw new IllegalArgumentException("Invalid identity source registry", error);
    }
  }

  /** Normalizes datasets while preserving the producer's Job, Run and parent identities. */
  public List<OpenLineage.RunEvent> normalizeEvents(OpenLineage.RunEvent event, String sourceHint)
      throws IOException {
    return Collections.singletonList(
        OpenLineageClientUtils.runEventFromJson(
            normalizeJson(OpenLineageClientUtils.toJson(event), sourceHint)));
  }

  public String normalizeJson(String json, String sourceHint) throws IOException {
    return MAPPER.writeValueAsString(normalizeTree(json, sourceHint));
  }

  private ObjectNode normalizeTree(String json, String sourceHint) throws IOException {
    JsonNode parsed = MAPPER.readTree(json);
    if (!(parsed instanceof ObjectNode))
      throw new IllegalArgumentException("RunEvent must be a JSON object");
    ObjectNode event = (ObjectNode) parsed;
    Map<String, Identity> identities = new HashMap<>();
    Map<String, Identity> resolved = new HashMap<>();
    mapDatasets(event.path("inputs"), identities, resolved, sourceHint);
    mapDatasets(event.path("outputs"), identities, resolved, sourceHint);
    for (JsonNode output : event.path("outputs")) {
      JsonNode lineage = output.path("facets").path("columnLineage");
      mapInputFields(lineage.path("dataset"), identities, resolved);
      JsonNode fields = lineage.path("fields");
      if (fields.isObject()) {
        for (JsonNode field : fields)
          mapInputFields(field.path("inputFields"), identities, resolved);
      }
    }
    return event;
  }

  private void mapDatasets(
      JsonNode datasets,
      Map<String, Identity> identities,
      Map<String, Identity> resolved,
      String sourceHint)
      throws IOException {
    if (!datasets.isArray()) return;
    for (JsonNode dataset : datasets) {
      if (!(dataset instanceof ObjectNode)) continue;
      removeLocalFileTableAliases((ObjectNode) dataset);
      String oldNamespace = dataset.path("namespace").asText();
      String oldName = dataset.path("name").asText();
      if (!isTable(dataset)) continue;
      Source source = sourceFor(dataset, sourceHint);
      if (source == null) {
        throw new IllegalArgumentException(
            "No authoritative source for table " + oldNamespace + "/" + oldName);
      }
      String table = tableFromSymlink(dataset);
      if (table == null) table = tableName(oldName);
      if (table == null) throw new IllegalArgumentException("Unsupported table name: " + oldName);
      boolean dropped =
          "DROP"
              .equalsIgnoreCase(
                  dataset
                      .path("facets")
                      .path("lifecycleStateChange")
                      .path("lifecycleStateChange")
                      .asText(""));
      Identity identity = resolve(source, table, resolved, dropped);
      Identity previous = identities.putIfAbsent(key(oldNamespace, oldName), identity);
      if (previous != null && !previous.equals(identity)) {
        throw new IllegalArgumentException(
            "Conflicting identities for " + oldNamespace + "/" + oldName);
      }
      ((ObjectNode) dataset).put("namespace", identity.namespace).put("name", identity.name);
      ((ObjectNode) dataset).with("facets").remove("symlinks");
      JsonNode dataSource = dataset.path("facets").path("dataSource");
      if (dataSource instanceof ObjectNode) {
        String uri =
            source.namespace.startsWith("hive://")
                ? "thrift://" + source.namespace.substring("hive://".length())
                : source.namespace;
        if (uri.startsWith("gravitino://")) {
          if (source.dynamicCatalogs.size() != 1) {
            throw new IllegalArgumentException("Ambiguous catalog metadata source");
          }
          uri = source.dynamicCatalogs.get(0).metadataUrl();
        }
        ((ObjectNode) dataSource).put("name", uri).put("uri", uri);
      }
    }
  }

  private static void removeLocalFileTableAliases(ObjectNode dataset) {
    String namespace = dataset.path("namespace").asText();
    if (!"file".equals(namespace) && !namespace.startsWith("file:")) return;
    JsonNode facets = dataset.path("facets");
    JsonNode identifiers = facets.path("symlinks").path("identifiers");
    if (!identifiers.isArray()) return;
    // Keep the physical file identity without publishing a second warehouse namespace.
    for (int i = identifiers.size() - 1; i >= 0; i--) {
      JsonNode link = identifiers.get(i);
      if ("TABLE".equals(link.path("type").asText())
          && link.path("namespace").asText().startsWith("file:")) {
        ((ArrayNode) identifiers).remove(i);
      }
    }
    if (identifiers.isEmpty()) ((ObjectNode) facets).remove("symlinks");
  }

  private static Identity resolve(Source source, String table, Map<String, Identity> resolved)
      throws IOException {
    return resolve(source, table, resolved, false);
  }

  private static Identity resolve(
      Source source, String table, Map<String, Identity> resolved, boolean dropped)
      throws IOException {
    String lookupKey = key(source.id, table);
    Identity cached = resolved.get(lookupKey);
    if (cached != null) return cached;
    Identity identity;
    if (!source.dynamicCatalogs.isEmpty()) {
      identity = null;
      for (ManagedCatalog catalog : source.dynamicCatalogs) {
        Identity candidate = catalog.resolve(table);
        if (candidate == null) continue;
        if (identity != null) {
          throw new IllegalArgumentException(
              "Table belongs to multiple Gravitino catalogs: " + table);
        }
        identity = candidate;
      }
      if (identity == null && dropped) {
        if (source.dynamicCatalogs.size() != 1) {
          throw new IllegalArgumentException("Ambiguous dropped table source: " + table);
        }
        ManagedCatalog catalog = source.dynamicCatalogs.get(0);
        identity = new Identity(catalog.identity.namespace, catalog.identity.name + "." + table);
      }
    } else {
      identity =
          source.managedCatalog == null
              ? source.managedTables.get(table)
              : source.managedCatalog.resolve(table);
    }
    if (identity == null) {
      if (source.namespace.startsWith("gravitino://")) {
        throw new IllegalArgumentException("No physical source for external table " + table);
      }
      identity = new Identity(source.namespace, table);
    }
    resolved.put(lookupKey, identity);
    return identity;
  }

  private Source sourceFor(JsonNode dataset, String sourceHint) throws IOException {
    Source result = null;
    JsonNode facets = dataset.path("facets");
    JsonNode dataSource = facets.path("dataSource");
    String[] candidates = {
      dataSource.path("uri").asText(""),
      dataSource.path("name").asText(""),
      dataset.path("namespace").asText("")
    };
    for (String candidate : candidates) result = combine(result, lookup(candidate));
    for (JsonNode link : facets.path("symlinks").path("identifiers")) {
      if ("TABLE".equals(link.path("type").asText())) {
        String linkNamespace = link.path("namespace").asText("");
        if (isStorageLocation(linkNamespace)) continue;
        Source linkedSource = lookup(linkNamespace);
        if (linkedSource == null) {
          throw new IllegalArgumentException("No authoritative source for TABLE symlink");
        }
        result = combine(result, linkedSource);
      }
    }
    if (result == null
        && sourceHint != null
        && isTable(dataset)
        && "hive".equalsIgnoreCase(dataset.path("namespace").asText(""))) {
      result = lookup(sourceHint);
    }
    return result;
  }

  private static Source combine(Source current, Source found) {
    if (current != null && found != null && current != found) {
      throw new IllegalArgumentException("Conflicting data sources in one dataset");
    }
    return found == null ? current : found;
  }

  private Source lookup(String value) throws IOException {
    if (value == null || value.isEmpty()) return null;
    if (discovery != null) return discovery.source(value);
    Source canonical = canonicalNamespaces.get(value);
    if (canonical != null) return canonical;
    try {
      return aliases.get(sourceKey(value));
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  private boolean isTable(JsonNode dataset) throws IOException {
    for (JsonNode link : dataset.path("facets").path("symlinks").path("identifiers")) {
      if ("TABLE".equals(link.path("type").asText())
          && !isStorageLocation(link.path("namespace").asText(""))) return true;
    }
    String namespace = dataset.path("namespace").asText("");
    String name = dataset.path("name").asText("");
    if (tableName(name) == null) return false;
    if (namespace.startsWith("http://") || namespace.startsWith("https://")) {
      Source source = lookup(namespace);
      return source != null && !source.dynamicCatalogs.isEmpty();
    }
    if (namespace.startsWith("hive://")
        || namespace.startsWith("thrift://")
        || namespace.startsWith("mysql://")
        || namespace.startsWith("postgres://")
        || namespace.startsWith("postgresql://")
        || "hive".equalsIgnoreCase(namespace)) return true;
    if (namespace.contains("://")) return false;
    JsonNode dataSource = dataset.path("facets").path("dataSource");
    return lookup(namespace) != null
        || lookup(dataSource.path("uri").asText("")) != null
        || lookup(dataSource.path("name").asText("")) != null
        || isKnownManagedIdentity(namespace, name);
  }

  private boolean isKnownManagedIdentity(String namespace, String name) throws IOException {
    if (managedIdentities.contains(new Identity(namespace, name))) return true;
    int separator = name.indexOf('.');
    if (separator < 0 || tableName(name).equals(name)) return false;
    return knownManagedCatalog(namespace, name.substring(0, separator)) != null;
  }

  private ManagedCatalog knownManagedCatalog(String namespace, String catalogName)
      throws IOException {
    if (discovery != null) {
      Source source = discovery.knownCatalog(namespace, catalogName);
      if (source != null) {
        for (ManagedCatalog catalog : source.dynamicCatalogs) {
          if (catalog.identity.namespace.equals(namespace)
              && catalog.identity.name.equals(catalogName)) return catalog;
        }
      }
    } else {
      for (Source source : canonicalNamespaces.values()) {
        ManagedCatalog catalog = source.managedCatalog;
        if (catalog != null
            && catalog.identity.namespace.equals(namespace)
            && catalog.identity.name.equals(catalogName)) return catalog;
      }
    }
    return null;
  }

  private static String tableName(String name) {
    if (name.matches("[^./:]+\\.[^./:]+")) return name;
    if (name.matches("[^./:]+\\.[^./:]+\\.[^./:]+")) return name.substring(name.indexOf('.') + 1);
    return null;
  }

  private static String tableFromSymlink(JsonNode dataset) {
    String table = null;
    for (JsonNode link : dataset.path("facets").path("symlinks").path("identifiers")) {
      if (!"TABLE".equals(link.path("type").asText())) continue;
      if (isStorageLocation(link.path("namespace").asText(""))) continue;
      String candidate = tableName(link.path("name").asText(""));
      if (candidate == null || (table != null && !table.equals(candidate))) {
        throw new IllegalArgumentException("Ambiguous TABLE symlink");
      }
      table = candidate;
    }
    return table;
  }

  private static boolean isStorageLocation(String namespace) {
    return namespace.startsWith("file:")
        || namespace.startsWith("hdfs:")
        || namespace.startsWith("s3:")
        || namespace.startsWith("s3a:");
  }

  private void mapInputFields(
      JsonNode fields, Map<String, Identity> identities, Map<String, Identity> resolved)
      throws IOException {
    if (!fields.isArray()) return;
    for (JsonNode field : fields) {
      String namespace = field.path("namespace").asText();
      String name = field.path("name").asText();
      Identity identity = identities.get(key(namespace, name));
      if (identity == null && !namespace.contains("://") && name.matches("[^.]+\\.[^.]+\\.[^.]+")) {
        String catalogName = name.substring(0, name.indexOf('.'));
        ManagedCatalog catalog = knownManagedCatalog(namespace, catalogName);
        if (catalog != null) identity = catalog.resolve(tableName(name));
      }
      if (identity == null && isTable(field)) {
        Source source = lookup(namespace);
        if (source != null) {
          String table = tableName(name);
          identity = resolve(source, table, resolved);
        }
      }
      if (identity != null && field instanceof ObjectNode) {
        ((ObjectNode) field).put("namespace", identity.namespace).put("name", identity.name);
      } else if (isTable(field)) {
        throw new IllegalArgumentException(
            "Unresolved column lineage source: " + namespace + "/" + name);
      }
    }
  }

  private static String key(String namespace, String name) {
    return namespace + "\u0000" + name;
  }

  private static String sourceKey(String text) {
    URI uri = URI.create(text.trim());
    if (uri.getHost() == null
        || uri.getScheme() == null
        || uri.getUserInfo() != null
        || uri.getFragment() != null) {
      throw new IllegalArgumentException("Source alias must be an absolute URI: " + text);
    }
    String scheme = uri.getScheme().equalsIgnoreCase("thrift") ? "hive" : uri.getScheme();
    String path = uri.getPath() == null ? "" : uri.getPath().replaceAll("/+$", "");
    return scheme.toLowerCase(Locale.ROOT)
        + "://"
        + uri.getHost().toLowerCase(Locale.ROOT)
        + (uri.getPort() < 0 ? "" : ":" + uri.getPort())
        + path;
  }

  private static boolean isSupportedSourceScheme(String scheme) {
    return "hive".equalsIgnoreCase(scheme)
        || "thrift".equalsIgnoreCase(scheme)
        || "mysql".equalsIgnoreCase(scheme)
        || "postgres".equalsIgnoreCase(scheme)
        || "postgresql".equalsIgnoreCase(scheme)
        || "http".equalsIgnoreCase(scheme)
        || "https".equalsIgnoreCase(scheme);
  }

  private static String required(JsonNode node, String property) {
    String value = node.path(property).asText("").trim();
    if (value.isEmpty()) throw new IllegalArgumentException("Missing source " + property);
    return value;
  }

  private static final class Source {
    String id;
    String namespace;
    final Map<String, Identity> managedTables = new HashMap<>();
    final ManagedCatalog managedCatalog;
    final List<ManagedCatalog> dynamicCatalogs = new ArrayList<>();

    Source(String namespace) {
      id = namespace;
      this.namespace = namespace;
      managedCatalog = null;
    }

    Source(JsonNode entry) {
      id = required(entry, "id");
      namespace = required(entry, "namespace");
      if (!entry.path("aliases").isArray() || entry.path("aliases").size() == 0) {
        throw new IllegalArgumentException("Source must have aliases: " + id);
      }
      if (entry.has("managedCatalog") && entry.path("managedTables").size() > 0) {
        throw new IllegalArgumentException("Use managedCatalog or managedTables for source: " + id);
      }
      managedCatalog =
          entry.has("managedCatalog") ? new ManagedCatalog(entry.path("managedCatalog")) : null;
      for (JsonNode table : entry.path("managedTables")) {
        String name = required(table, "name");
        if (!name.matches("[^.]+\\.[^.]+"))
          throw new IllegalArgumentException("Managed table must be database.table: " + name);
        Identity identity =
            new Identity(required(table, "metalake"), required(table, "catalog") + "." + name);
        if (managedTables.putIfAbsent(name, identity) != null)
          throw new IllegalArgumentException("Duplicate managed table: " + name);
      }
    }
  }

  private static final class ManagedCatalog {
    final Identity identity;
    final String url;
    final GravitinoCatalogDiscovery discovery;

    ManagedCatalog(JsonNode entry) {
      discovery = null;
      identity = new Identity(required(entry, "metalake"), required(entry, "catalog"));
      String configuredUrl = required(entry, "gravitinoUrl");
      URI uri = URI.create(configuredUrl);
      if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
          || uri.getHost() == null
          || uri.getUserInfo() != null
          || uri.getQuery() != null
          || uri.getFragment() != null) {
        throw new IllegalArgumentException("Invalid Gravitino URL: " + configuredUrl);
      }
      url = configuredUrl.replaceAll("/+$", "");
    }

    ManagedCatalog(String metalake, String catalog, GravitinoCatalogDiscovery discovery) {
      identity = new Identity(metalake, catalog);
      url = discovery.url();
      this.discovery = discovery;
    }

    String metadataUrl() throws IOException {
      return url
          + "/api/metalakes/"
          + encode(identity.namespace)
          + "/catalogs/"
          + encode(identity.name);
    }

    Identity resolve(String table) throws IOException {
      String[] parts = table.split("\\.", -1);
      String catalogPath =
          "/api/metalakes/" + encode(identity.namespace) + "/catalogs/" + encode(identity.name);
      String tablePath =
          catalogPath + "/schemas/" + encode(parts[0]) + "/tables/" + encode(parts[1]);
      if (exists(tablePath)) return new Identity(identity.namespace, identity.name + "." + table);
      if (!exists(catalogPath)) {
        throw new IOException("Gravitino managed catalog is missing: " + identity.name);
      }
      return null;
    }

    private boolean exists(String path) throws IOException {
      if (discovery != null) return discovery.get(path) != null;
      HttpURLConnection connection = (HttpURLConnection) new URL(url + path).openConnection();
      connection.setConnectTimeout(2000);
      connection.setReadTimeout(2000);
      connection.setRequestMethod("GET");
      String authorization = System.getenv("OPENLINEAGE_GRAVITINO_AUTHORIZATION");
      if (authorization != null && !authorization.isEmpty()) {
        connection.setRequestProperty("Authorization", authorization);
      }
      try {
        int status = connection.getResponseCode();
        if (status == 404) return false;
        if (status != 200) throw new IOException("Gravitino lookup returned HTTP " + status);
        JsonNode response = MAPPER.readTree(connection.getInputStream());
        if (response == null || response.path("code").asInt(-1) != 0) {
          throw new IOException("Gravitino lookup returned an invalid response");
        }
        return true;
      } finally {
        connection.disconnect();
      }
    }

    private static String encode(String value) throws IOException {
      return URLEncoder.encode(value, "UTF-8").replace("+", "%20");
    }
  }

  private static final class GravitinoCatalogDiscovery {
    private static final long REFRESH_MILLIS = 30_000;
    private final HttpConfig transport;
    private final String baseUrl;
    private Map<String, Source> sources = new HashMap<>();
    private Map<String, Source> catalogSources = new HashMap<>();
    private long expiresAt;

    GravitinoCatalogDiscovery(HttpConfig transport) {
      this.transport = transport;
      URI url = transport.getUrl();
      if (url == null
          || url.getHost() == null
          || !("http".equalsIgnoreCase(url.getScheme())
              || "https".equalsIgnoreCase(url.getScheme()))) {
        throw new IllegalArgumentException("Gravitino transport must have an HTTP URL");
      }
      String path = url.getPath() == null ? "" : url.getPath();
      if (path.endsWith("/api/lineage")) path = path.substring(0, path.length() - 12);
      baseUrl = url.getScheme() + "://" + url.getAuthority() + path.replaceAll("/+$", "");
    }

    String url() {
      return baseUrl;
    }

    synchronized Source source(String value) throws IOException {
      if (value.startsWith("gravitino://")) {
        URI catalogUri;
        try {
          catalogUri = URI.create(value);
        } catch (IllegalArgumentException error) {
          throw new IllegalArgumentException(
              "Invalid Gravitino catalog identifier: " + value, error);
        }
        if (catalogUri.getRawAuthority() == null
            || catalogUri.getRawAuthority().isEmpty()
            || catalogUri.getPath() == null
            || !catalogUri.getPath().matches("/[^/]+")
            || catalogUri.getQuery() != null
            || catalogUri.getFragment() != null) {
          throw new IllegalArgumentException("Invalid Gravitino catalog identifier: " + value);
        }
        if (System.currentTimeMillis() >= expiresAt) refresh();
        Source catalog = catalogSources.get(value);
        if (catalog == null)
          throw new IllegalArgumentException("Unknown Gravitino catalog: " + value);
        return catalog;
      }
      String key;
      try {
        key = sourceKey(value.startsWith("jdbc:") ? value.substring(5) : value);
      } catch (IllegalArgumentException error) {
        return null;
      }
      String scheme = URI.create(key).getScheme();
      if (!isSupportedSourceScheme(scheme)) return null;
      if (System.currentTimeMillis() >= expiresAt) refresh();
      Source source = sources.get(key);
      if (source == null) {
        source = new Source(key);
        sources.put(key, source);
      }
      return source;
    }

    synchronized Source knownCatalog(String metalake, String catalog) throws IOException {
      if (System.currentTimeMillis() >= expiresAt) refresh();
      return catalogSources.get("gravitino://" + metalake + "/" + catalog);
    }

    private void refresh() throws IOException {
      Map<String, Source> found = new HashMap<>();
      Map<String, Source> foundCatalogs = new HashMap<>();
      for (String metalake : names(getRequired("/api/metalakes"), "metalakes")) {
        String metalakePath = "/api/metalakes/" + encode(metalake);
        for (String catalog : names(getRequired(metalakePath + "/catalogs"), "catalogs")) {
          String catalogPath = metalakePath + "/catalogs/" + encode(catalog);
          JsonNode detail = getRequired(catalogPath).path("catalog");
          if (!detail.isObject()) throw new IOException("Invalid Gravitino catalog response");
          JsonNode properties = detail.path("properties");
          Set<String> keys = new TreeSet<>();
          addUris(keys, properties.path("metastore.uris").asText(""));
          addUris(keys, properties.path("uri").asText(""));
          addUris(keys, properties.path("jdbc-url").asText(""));
          addUris(keys, properties.path("jdbc.url").asText(""));
          addUris(keys, properties.path("openlineage.source.aliases").asText(""));
          if (keys.isEmpty() && "hive".equalsIgnoreCase(detail.path("provider").asText())) {
            throw new IOException("Hive catalog has no metastore URI: " + catalog);
          }
          Source source = null;
          for (String key : keys) {
            Source existing = found.get(key);
            if (existing == null) continue;
            if (source == null) source = existing;
            else if (source != existing) {
              Source target = source;
              Source duplicate = existing;
              found.replaceAll((alias, current) -> current == duplicate ? target : current);
              foundCatalogs.replaceAll((alias, current) -> current == duplicate ? target : current);
              target.dynamicCatalogs.addAll(duplicate.dynamicCatalogs);
              target.namespace =
                  target.namespace.compareTo(duplicate.namespace) < 0
                      ? target.namespace
                      : duplicate.namespace;
              target.id = target.namespace;
            }
          }
          String catalogKey = "gravitino://" + metalake + "/" + catalog;
          if (source == null)
            source = new Source(keys.isEmpty() ? catalogKey : keys.iterator().next());
          source.dynamicCatalogs.add(new ManagedCatalog(metalake, catalog, this));
          if (!keys.isEmpty()) {
            source.namespace =
                source.namespace.compareTo(keys.iterator().next()) < 0
                    ? source.namespace
                    : keys.iterator().next();
          }
          source.id = source.namespace;
          for (String key : keys) found.put(key, source);
          foundCatalogs.put(catalogKey, source);
        }
      }
      sources = found;
      catalogSources = foundCatalogs;
      expiresAt = System.currentTimeMillis() + REFRESH_MILLIS;
    }

    private static void addUris(Set<String> keys, String values) throws IOException {
      for (String value : values.split(",")) {
        String candidate = value.trim();
        if (candidate.startsWith("jdbc:")) candidate = candidate.substring(5);
        if (candidate.isEmpty()) continue;
        try {
          String scheme = URI.create(candidate).getScheme();
          if (!isSupportedSourceScheme(scheme)) continue;
          keys.add(sourceKey(candidate));
        } catch (IllegalArgumentException error) {
          throw new IOException("Invalid Gravitino catalog source URI: " + candidate, error);
        }
      }
    }

    private static List<String> names(JsonNode response, String field) throws IOException {
      JsonNode items = response.path(field);
      if (!items.isArray()) items = response.path("identifiers");
      if (!items.isArray()) throw new IOException("Invalid Gravitino " + field + " response");
      List<String> result = new ArrayList<>();
      for (JsonNode item : items) {
        String name = item.isTextual() ? item.asText() : item.path("name").asText("");
        if (name.isEmpty()) name = item.path("catalog").path("name").asText("");
        if (name.isEmpty()) name = item.path("metalake").path("name").asText("");
        if (name.isEmpty()) throw new IOException("Invalid Gravitino " + field + " name");
        result.add(name);
      }
      return result;
    }

    private JsonNode getRequired(String path) throws IOException {
      JsonNode response = get(path);
      if (response == null) throw new IOException("Gravitino resource is missing: " + path);
      return response;
    }

    JsonNode get(String path) throws IOException {
      HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + path).openConnection();
      connection.setConnectTimeout(2000);
      connection.setReadTimeout(2000);
      connection.setRequestMethod("GET");
      if (transport.getHeaders() != null) {
        transport.getHeaders().forEach(connection::setRequestProperty);
      }
      if (transport.getAuth() != null) {
        connection.setRequestProperty("Authorization", transport.getAuth().getToken());
      }
      try {
        int status = connection.getResponseCode();
        if (status == 404) return null;
        if (status != 200) throw new IOException("Gravitino lookup returned HTTP " + status);
        try (InputStream input = connection.getInputStream()) {
          JsonNode response = MAPPER.readTree(input);
          if (response == null || response.path("code").asInt(-1) != 0) {
            throw new IOException("Gravitino lookup returned an invalid response");
          }
          return response;
        }
      } finally {
        connection.disconnect();
      }
    }

    private static String encode(String value) throws IOException {
      return URLEncoder.encode(value, "UTF-8").replace("+", "%20");
    }
  }

  private static final class Identity {
    final String namespace;
    final String name;

    Identity(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Identity)) return false;
      Identity identity = (Identity) other;
      return namespace.equals(identity.namespace) && name.equals(identity.name);
    }

    @Override
    public int hashCode() {
      return key(namespace, name).hashCode();
    }
  }
}
