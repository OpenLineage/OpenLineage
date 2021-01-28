package io.openlineage.client;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Generator {

  public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
    File f = new File("/Users/julien/github/OpenLineage/OpenLineage/spec/OpenLineage.json");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode schema = mapper.readValue(f, JsonNode.class);
    TypeResolver typeResolver = new TypeResolver(schema);
    new JavaGenerator(typeResolver).generate();
    new JavaPoetGenerator(typeResolver).generate();

  }


}
