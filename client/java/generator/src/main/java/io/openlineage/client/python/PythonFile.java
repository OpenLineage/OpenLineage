package io.openlineage.client.python;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class PythonFile implements Dump {

  public Set<TypeRef> requirements = new HashSet<>();
  public Map<String, ClassSpec> dumpables = new HashMap<>();
  public Map<String, Set<TypeRef>> dumpableDependencies = new HashMap<>();

  public void dumpToFile(Path path) {
    try {
      Files.write(path, dump(0).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addClass(ClassSpec clazz) {
    dumpables.put(clazz.name, clazz);
    requirements.addAll(clazz.requirements);
    dumpableDependencies.put(clazz.name, new HashSet<>(clazz.getTypeDependencies()));
  }

  public String dump(int nestLevel) {

    // Naive toposort this
    List<Dump> sortedDumpables = new ArrayList<>();
    Queue<String> classes = new ArrayDeque<>(dumpables.keySet());
    while (!classes.isEmpty()) {
      String className = classes.poll();
      System.out.printf("Class: %s ", className);
      System.out.printf(
          "remaining deps: %s\n",
          dumpableDependencies.get(className).stream()
              .map(TypeRef::getName)
              .collect(Collectors.toList()));
      System.out.flush();
      if (dumpableDependencies.get(className).isEmpty()) {
        ClassSpec clazz = dumpables.get(className);
        sortedDumpables.add(dumpables.get(className));
        for (String key : dumpableDependencies.keySet()) {
          Set<TypeRef> typeRefs = dumpableDependencies.get(key);
          typeRefs.remove(clazz.getTypeRef());
          dumpableDependencies.put(key, typeRefs);
        }
        System.out.flush();
      } else {
        if (!requirements.contains(className)) {
          System.out.printf("Not adding class: %s\n", className);
          System.out.flush();
          classes.add(className);
        }
      }
    }

    StringBuilder content = new StringBuilder();
    for (TypeRef requirement : requirements) {
      if (requirement.isPrimitive || requirement.isInternal) {
        continue;
      }

      if (requirement.getModule() == null) {
        content.append(String.format("import %s", requirement.getName()));
      }
      else {
        content.append(String.format("from %s import %s", requirement.getModule(), requirement.getName()));
      }
      content.append("\n");
    }
    content.append("\n\n");

    for (Dump dumpable : sortedDumpables) {
      content.append(dumpable.dump(nestLevel));
      content.append("\n\n");
    }
    return content.toString();
  }
}
