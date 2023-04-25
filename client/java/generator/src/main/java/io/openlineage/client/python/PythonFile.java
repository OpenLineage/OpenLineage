package io.openlineage.client.python;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class PythonFile {

  public Set<TypeRef> requirements = new HashSet<>();
  public Map<TypeRef, ClassSpec> dumpables = new HashMap<>();
  public Map<TypeRef, Set<TypeRef>> dumpableDependencies = new HashMap<>();

  public void addClass(ClassSpec clazz) {
    if (clazz.getName().equals("BaseFacet")) {
      System.out.printf("BaseFacet Reqs: %s\n", Arrays.toString(clazz.requirements.toArray()));
      System.out.printf(
          "BaseFacet Type Deps: %s\n", Arrays.toString(clazz.getTypeDependencies().toArray()));
    }
    dumpables.put(clazz.getTypeRef(), clazz);
    requirements.addAll(clazz.requirements);
    dumpableDependencies.put(clazz.getTypeRef(), new HashSet<>(clazz.getTypeDependencies()));
  }

  public String dump(int nestLevel, Map<TypeRef, TypeRef> parentClassMapping) {
    int addJustSome = 9999;

    System.out.printf(
        "%s\n", Arrays.toString(requirements.stream().map(TypeRef::getName).toArray()));
    System.out.printf(
        "%s\n",
        Arrays.toString(parentClassMapping.keySet().stream().map(TypeRef::getName).toArray()));
    System.out.printf(
        "%s\n", Arrays.toString(dumpables.keySet().stream().map(TypeRef::getName).toArray()));

    // Remove dead reqs and create queue
    for (TypeRef deadRef : parentClassMapping.keySet()) {
      System.out.printf("Dead ref class: %s\n", deadRef.getName());
      for (TypeRef dumpable : dumpableDependencies.keySet()) {
        Set<TypeRef> typeRefs = dumpableDependencies.get(dumpable);
        if (!typeRefs.contains(deadRef)) {
          continue;
        }
        System.out.printf("\tRemoving ref in class %s\n", dumpable.getName());
        typeRefs.remove(deadRef);
        if (parentClassMapping.get(deadRef) != null) {
          System.out.printf(
              "\t\tAdding ref to parent %s\n", parentClassMapping.get(deadRef).getName());
          typeRefs.add(parentClassMapping.get(deadRef));
        }
        dumpableDependencies.put(dumpable, typeRefs);
      }
    }

    // Naive toposort this
    List<ClassSpec> sortedDumpables = new ArrayList<>();
    Queue<TypeRef> classes = new ArrayDeque<>(dumpables.keySet());
    while (!classes.isEmpty()) {
      TypeRef classRef = classes.poll();
      //      System.out.printf("Class: %s ", classRef.getName());
      //      System.out.printf(
      //          "remaining deps: %s\n",
      //          dumpableDependencies.get(classRef).stream()
      //            .filter(Objects::nonNull)
      //            .map(TypeRef::getName)
      //            .collect(Collectors.toList()));
      System.out.flush();
      if (dumpableDependencies.get(classRef).isEmpty()) {
        ClassSpec clazz = dumpables.get(classRef);
        sortedDumpables.add(dumpables.get(classRef));
        for (TypeRef type : dumpableDependencies.keySet()) {
          Set<TypeRef> typeRefs = dumpableDependencies.get(type);
          typeRefs.remove(clazz.getTypeRef());
          dumpableDependencies.put(type, typeRefs);
        }
        //        System.out.printf("Dumped class %s\n", classRef.getName());
        //        System.out.flush();
      } else {
        if (parentClassMapping.containsKey(classRef)) {
          //          System.out.printf("Not requeueing class: %s - parentClassMapping\n",
          // classRef.getName());
          ClassSpec clazz = dumpables.get(classRef);
          for (TypeRef type : dumpableDependencies.keySet()) {
            Set<TypeRef> typeRefs = dumpableDependencies.get(type);
            typeRefs.remove(clazz.getTypeRef());
            dumpableDependencies.put(type, typeRefs);
          }
          continue;
        }

        if (requirements.contains(classRef)) {
          System.out.printf("Not requeueing class: %s - is a req\n", classRef.getName());
          continue;
        }
        System.out.printf(
            "Requeueing class: %s - has reqs %s\n",
            classRef.getName(),
            Arrays.toString(
                dumpableDependencies.get(classRef).stream().map(TypeRef::getName).toArray()));
        System.out.flush();

        if (addJustSome > 0) {
          addJustSome--;
          classes.add(classRef);
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
      } else {
        content.append(
            String.format("from %s import %s", requirement.getModule(), requirement.getName()));
      }
      content.append("\n");
    }
    content.append("\n\n");

    for (ClassSpec dumpable : sortedDumpables) {
      content.append(dumpable.dump(nestLevel, parentClassMapping));
      content.append("\n\n");
    }
    return content.toString();
  }
}
