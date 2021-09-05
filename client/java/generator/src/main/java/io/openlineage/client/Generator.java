package io.openlineage.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private static final String JSON_EXT = ".json";

  /**
   * will generate java classes from the spec URL
   * @param args either empty or a single argument: the url to the spec to generate
   * @throws JsonParseException if the spec is not valid
   * @throws JsonMappingException if the spec is not valid
   * @throws IOException if the spec can't be read or class can not be generated
   */
  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
    List<String> baseURLs = Arrays.asList(args);
    List<URL> urls = new ArrayList<URL>(baseURLs.size());
    for (String baseURL : baseURLs) {
      urls.add(new URL(baseURL));
    }
    generate(urls, new File("src/main/java/io/openlineage/client/"));
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
          throw new RuntimeException("You must increment the version when modifying the schema. The current schema at " + url + " has the $id " + idURL + " but the version at that URL does not match.");
        }
      } catch (FileNotFoundException e) {
        logger.warn("This version of the spec is not published yet: " + e.toString());
      }
      return idURL;
    }
    return url;
  }

  public static void generate(List<URL> urls, File outputBase) {
    if (!outputBase.exists()) {
      if (!outputBase.mkdirs()) {
        throw new RuntimeException("can't create output " + outputBase.getAbsolutePath());
      }
    }
    try {
      TypeResolver typeResolver = new TypeResolver(urls);
      Map<String, ObjectResolvedType> facetContainers = indexFacetContainersByType(typeResolver);
      enrichFacetContainersWithFacets(typeResolver, facetContainers);
      for (URL url : urls) {
        if (!url.getPath().contains("OpenLineage.json")) {
          continue;
        }
        logger.info("Generating from URL: " + url);
        url = verifySchemaVersion(url);
        String path = url.getPath();
        if (path.endsWith(JSON_EXT)) {
          String containerClassName = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));
          String javaPath = containerClassName + ".java";
          File output = new File(outputBase, javaPath);
          try (PrintWriter printWriter = new PrintWriter(output)) {
            new JavaPoetGenerator(typeResolver, containerClassName, url).generate(printWriter);
          }
        } else {
          throw new IllegalArgumentException("inputs should end in " + JSON_EXT + " got " + path);
        }
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
      if (objectResolvedType.getContainer().equals("OpenLineage")) {
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
      if (objectResolvedType.getContainer().equals("OpenLineage")
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

}
