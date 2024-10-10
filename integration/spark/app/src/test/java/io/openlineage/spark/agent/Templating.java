/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
@UtilityClass
public class Templating {

  public String renderTemplate(String templatePath, Map<String, String> parameters) {
    return substituteParameters(parameters, readFileContent(templatePath));
  }

  private static String substituteParameters(
      Map<String, String> parameters, String templateContent) {
    String result = templateContent;

    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      result = result.replace("{{" + entry.getKey() + "}}", entry.getValue());
    }

    return result;
  }

  private static @NotNull String readFileContent(String templatePath) {
    String templateContent;
    try {
      String path = Resources.getResource(templatePath).getPath();
      log.debug("Reading template file: {}", path);
      templateContent = new String(Files.readAllBytes(Paths.get(path)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return templateContent;
  }
}
