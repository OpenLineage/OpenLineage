package io.openlineage.client;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Generator {

  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
    String baseURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json";
    File input = new File("../spec/OpenLineage.json");
    File output = new File("src/main/java/io/openlineage/client/OpenLineage.java");
    generate(baseURL, input, output);
  }

  public static void generate(String baseURL, File input, File output) {
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
