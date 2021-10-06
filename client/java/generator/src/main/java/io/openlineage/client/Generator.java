package io.openlineage.client;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.openlineage.client.TypeResolver.DefaultResolvedTypeVisitor;
import io.openlineage.client.TypeResolver.ObjectResolvedType;
import io.openlineage.client.TypeResolver.ResolvedField;

public class Generator {
  private static final Logger logger = LoggerFactory.getLogger(Generator.class);

  private static final String CONTAINER_CLASS_NAME = "OpenLineage";
  private static final String JSON_EXT = ".json";
  private static final String JAVA_EXT = ".java";

  /**
   * will generate java classes from the spec URL
   * @param args either empty or a single argument: the url to the spec to generate
   * @throws JsonParseException if the spec is not valid
   * @throws JsonMappingException if the spec is not valid
   * @throws IOException if the spec can't be read or class can not be generated
   * @throws URISyntaxException
   */
  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException, URISyntaxException {
    List<String> baseURLs = Arrays.asList(args);
    Set<URL> urls = new LinkedHashSet<>();
    for (String baseURL : baseURLs) {
      URL url = new URL(baseURL);

      if (url.getProtocol().equals("file")) {
        File file = new File(url.toURI());
        if (file.isDirectory()) {
          addURLs(urls, file);
        } else {
          urls.add(url);
        }
      } else {
        urls.add(url);
      }
    }
    logger.info("Generating code for schemas:\n" + urls.stream().map(Object::toString).collect(Collectors.joining("\n")));
    generate(new HashSet<>(asList(urls.iterator().next())), "io.openlineage.server", true, new File("src/main/java/io/openlineage/server/"));
    generate(urls, "io.openlineage.client", false, new File("src/main/java/io/openlineage/client/"));
  }

  private static void addURLs(Set<URL> urls, File dir)
      throws URISyntaxException, MalformedURLException {
    File[] jsonFiles = dir.listFiles((File f, String name) -> name.endsWith(JSON_EXT));
    for (File jsonFile : jsonFiles) {
      urls.add(jsonFile.toURI().toURL());
    }
    File[] subDirs = dir.listFiles(File::isDirectory);
    for (File subDir : subDirs) {
      addURLs(urls, subDir);
    }
  }

  public static URL verifySchemaVersion(URL url) throws IOException {
    InputStream input = url.openStream();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode schema = mapper.readValue(input, JsonNode.class);
    if (schema.has("$id") && schema.get("$id").isTextual()) {
      URL idURL = new URL(schema.get("$id").asText());
      try (InputStream openStream = idURL.openStream();) {
        JsonNode published = mapper.readValue(openStream, JsonNode.class);
        if (!published.equals(schema)) {
          throw new InvalidSchemaIDException("You must increment the version when modifying the schema. The current schema at " + url + " has the $id " + idURL + " but the version at that URL does not match.");
        }
      } catch (FileNotFoundException e) {
        logger.warn("This version of the spec is not published yet: " + idURL);
      }
      return idURL;
    }
    return url;
  }

  public static void generate(Set<URL> urls, String packageName, boolean server, File outputBase) throws FileNotFoundException {
    if (!outputBase.exists()) {
      if (!outputBase.mkdirs()) {
        throw new FileNotFoundException("can't create output " + outputBase.getAbsolutePath());
      }
    }
    try {

      Map<String, URL> containerToID = new HashMap<>();
      for (URL url : urls) {
        String path = url.getPath();
        url = verifySchemaVersion(url);
        if (path.endsWith(JSON_EXT)) {
          String containerClassName = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
          containerToID.put(containerClassName, url);
        } else {
          throw new IllegalArgumentException("inputs should end in " + JSON_EXT + " got " + path);
        }
      }

      TypeResolver typeResolver = new TypeResolver(urls);

      if (server) {
        // The server wants to be forward compatible and read any extra fields.
        for (ObjectResolvedType objectResolvedType : typeResolver.getTypes()) {
          objectResolvedType.setAdditionalProperties(true);
        }
      } else {
        // We add the facets to the core model here to keep code generation convenient
        Map<String, ObjectResolvedType> facetContainers = indexFacetContainersByType(typeResolver);
        enrichFacetContainersWithFacets(typeResolver, facetContainers);
      }

      String javaPath = CONTAINER_CLASS_NAME + JAVA_EXT;
      File output = new File(outputBase, javaPath);
      try (PrintWriter printWriter = new PrintWriter(output)) {
        new JavaPoetGenerator(typeResolver, packageName, CONTAINER_CLASS_NAME, server, containerToID).generate(printWriter);
      }

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void enrichFacetContainersWithFacets(
      TypeResolver typeResolver, Map<String, ObjectResolvedType> facetContainers) {
    for (ObjectResolvedType objectResolvedType : typeResolver.getTypes()) {
      if (objectResolvedType.getContainer().equals(CONTAINER_CLASS_NAME)) {
        continue;
      }
      List<ResolvedField> properties = objectResolvedType.getProperties();
      for (ResolvedField property : properties) {
        property.getType().accept(new DefaultResolvedTypeVisitor<Void>() {
          @Override
          public Void visit(ObjectResolvedType objectType) {
            Set<ObjectResolvedType> parents = objectType.getParents();
            for (ObjectResolvedType parent : parents) {
              if (facetContainers.containsKey(parent.getName())) {
                facetContainers.get(parent.getName()).getProperties().add(property);
              }
            }
            return null;
          }
        });
      }
    }
  }

  private static Map<String, ObjectResolvedType> indexFacetContainersByType(
      TypeResolver typeResolver) {
    Map<String, ObjectResolvedType> facetContainers = new HashMap<>();
    for (ObjectResolvedType objectResolvedType : typeResolver.getTypes()) {
      if (objectResolvedType.getContainer().equals(CONTAINER_CLASS_NAME)
          && objectResolvedType.hasAdditionalProperties()
          && objectResolvedType.getAdditionalPropertiesType() != null) {
        objectResolvedType.getAdditionalPropertiesType().accept(new DefaultResolvedTypeVisitor<Void>() {
          @Override
          public Void visit(ObjectResolvedType objectType) {
            facetContainers.put(objectType.getName(), objectResolvedType);
            return null;
          }
        });
      }
    }
    return facetContainers;
  }

  public static class InvalidSchemaIDException extends RuntimeException {

    public InvalidSchemaIDException(String message) {
      super(message);
    }

    private static final long serialVersionUID = 1L;

  }

}
