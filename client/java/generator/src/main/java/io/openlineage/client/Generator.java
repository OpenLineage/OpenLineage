package io.openlineage.client;

import java.io.File;
import java.io.FileInputStream;
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
    InputStream input;
    if (baseURL == null) {
      String localSpec = "../spec/OpenLineage.json";
      logger.info("Generating from local file: " + localSpec);
      baseURL = "";
      input = new FileInputStream(localSpec);
    } else {
      logger.info("Generating from URL: " + baseURL);
      input = new URL(baseURL).openStream();
    }
    try {
      File output = new File("src/main/java/io/openlineage/client/OpenLineage.java");
      generate(baseURL, input, output);
    } finally {
      input.close();
    }
  }

  private static String singleArg(String[] args) {
    if (args.length == 0) {
      return null;
    } else if (args.length == 1) {
      return args[0];
    } else {
      throw new IllegalArgumentException("the argument should be the base URL, got several arguments instead: " + Arrays.toString(args));
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
      TypeResolver typeResolver = new TypeResolver(schema);
      try (PrintWriter printWriter = new PrintWriter(output)) {
        new JavaPoetGenerator(typeResolver, baseURL).generate(printWriter);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
