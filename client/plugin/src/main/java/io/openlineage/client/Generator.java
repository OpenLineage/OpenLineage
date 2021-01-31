package io.openlineage.client;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Generator {

  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
    File f = new File("../../spec/OpenLineage.json");
    String baseURL = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/jsonschema/spec/OpenLineage.json";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode schema = mapper.readValue(f, JsonNode.class);
    TypeResolver typeResolver = new TypeResolver(schema);
    new JavaPoetGenerator(typeResolver, baseURL).generate();
  }


}
