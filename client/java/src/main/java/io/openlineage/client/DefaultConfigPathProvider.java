package io.openlineage.client;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DefaultConfigPathProvider implements ConfigPathProvider {
  private static final String OPENLINEAGE_YML = "openlineage.yml";

  @Override
  public List<Path> getPaths() {
    List<Path> paths = new ArrayList<>();

    if (System.getenv("OPENLINEAGE_CONFIG") != null) {
      paths.add(Paths.get(System.getenv("OPENLINEAGE_CONFIG")));
    }

    if (System.getProperty("user.dir") != null) {
      paths.add(Paths.get(System.getProperty("user.dir") + File.separatorChar + OPENLINEAGE_YML));
    }
    if (System.getProperty("user.home") != null) {
      paths.add(
          Paths.get(
              System.getProperty("user.home")
                  + File.separatorChar
                  + ".openlineage"
                  + File.separatorChar
                  + OPENLINEAGE_YML));
    }
    return paths;
  }
}
