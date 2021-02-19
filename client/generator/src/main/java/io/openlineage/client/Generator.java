package io.openlineage.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Generator {

  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
    String branch = branchFromArgs(args);
    String baseURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/" + branch + "/spec/OpenLineage.json";
    InputStream input = new URL(baseURL).openStream();
    File output = new File("src/main/java/io/openlineage/client/OpenLineage.java");
    generate(baseURL, input, output);
  }

  private static String branchFromArgs(String[] args) {
    if (args.length == 0) {
      return "main";
    } else if (args.length == 1) {
      return args[0];
    } else {
      throw new IllegalArgumentException("the argument should be a branch name, got several arguments instead: " + Arrays.toString(args));
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
