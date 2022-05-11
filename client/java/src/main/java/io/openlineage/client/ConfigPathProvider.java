package io.openlineage.client;

import java.nio.file.Path;
import java.util.List;

public interface ConfigPathProvider {
  List<Path> getPaths();
}
