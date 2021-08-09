package io.openlineage.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Generator {
  private static final Logger logger = LoggerFactory.getLogger(Generator.class);

  /**
   * will generate java classes from the spec URL
   * @param args either empty or a single argument: the url to the spec to generate
   * @throws JsonParseException if the spec is not valid
   * @throws JsonMappingException if the spec is not valid
   * @throws IOException if the spec can't be read or class can not be generated
   */
  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
    String baseURL = singleArg(args);
    logger.info("Generating from URL: " + baseURL);
    InputStream input = new URL(baseURL).openStream();
    try {
      File output = new File("src/main/java/io/openlineage/client/OpenLineage.java");
      generate(baseURL, input, output);
    } finally {
      input.close();
    }
  }

  private static String singleArg(String[] args) {
    if (args.length == 1) {
      return args[0];
    } else {
      throw new IllegalArgumentException("the argument should be the base URL, got invalid arguments instead: " + Arrays.toString(args));
    }
  }

  public static void generate(String baseURL, InputStream input, File output) {
    if (!output.getParentFile().exists()) {
      if (!output.getParentFile().mkdirs()) {
        throw new RuntimeException("can't create output " + output.getAbsolutePath());
      }
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode schema = mapper.readValue(input, JsonNode.class);
      if (schema.has("$id") && schema.get("$id").isTextual()) {
        String idURL = schema.get("$id").asText();
        try (InputStream openStream = new URL(idURL).openStream();) {
          JsonNode published = mapper.readValue(openStream, JsonNode.class);
          if (!published.equals(schema)) {
            throw new RuntimeException("You must increment the version when modifying the schema. The current schema at " + baseURL + " has the $id " + idURL + " but the version at that URL does not match.");
          }
        } catch (FileNotFoundException e) {
          logger.warn("This version of the spec is not published yet: " + e.toString());
        } finally {
          baseURL = idURL;
        }
      }
      TypeResolver typeResolver = new TypeResolver(schema);
      try (PrintWriter printWriter = new PrintWriter(output)) {
        new JavaPoetGenerator(typeResolver, baseURL).generate(printWriter);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
