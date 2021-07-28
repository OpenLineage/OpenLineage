package io.openlineage.client;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

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

  public static void generate(List<URL> urls, File outputBase) {
    if (!outputBase.exists()) {
      if (!outputBase.mkdirs()) {
        throw new RuntimeException("can't create output " + outputBase.getAbsolutePath());
      }
    }
    try {
      TypeResolver typeResolver = new TypeResolver(urls);
      for (URL url : urls) {
        logger.info("Generating from URL: " + url);
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



    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
