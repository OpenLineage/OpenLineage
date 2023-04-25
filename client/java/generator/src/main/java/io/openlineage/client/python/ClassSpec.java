package io.openlineage.client.python;

import static io.openlineage.client.python.Utils.nestString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Getter
@Builder
public class ClassSpec {
  public String name;

  @Singular public List<TypeRef> parents;
  @Singular public List<FieldSpec> fields;
  @Singular public List<MethodSpec> methods;
  @Singular public List<DecoratorSpec> decorators;

  @Singular public Set<TypeRef> requirements;

  public boolean isEnum;

  public List<TypeRef> getTypeDependencies() {
    if (isEnum) {
      return Collections.emptyList();
    }
    List<TypeRef> typeRefs = new ArrayList<>();
    for (FieldSpec field : fields) {
      if (field.type.arrayRef != null) {
        if (!field.type.arrayRef.isPrimitive && field.type.arrayRef.isInternal) {
          typeRefs.add(field.type.arrayRef);
        }
      }

      if (!field.type.isPrimitive && field.type.isInternal) {
        typeRefs.add(field.type);
      }
    }
    for (TypeRef parent : parents) {
      if (!parent.isPrimitive && parent.isInternal) {
        typeRefs.add(parent);
      }
    }
    return typeRefs;
  }

  public TypeRef getTypeRef() {
    // TODO: module? sameFile?
    return TypeRef.builder().name(name).isInternal(true).build();
  }

  public String dump(int nestLevel, Map<TypeRef, TypeRef> parentClassMapping) {
    StringBuilder content = new StringBuilder();

    for (DecoratorSpec decorator : decorators) {
      content.append(decorator.dump(nestLevel));
      content.append("\n");
    }

    StringBuilder parentString = new StringBuilder();
    if (!parents.isEmpty()) {
      parentString.append("(");
      ListIterator<TypeRef> listIterator = parents.listIterator();
      while (listIterator.hasNext()) {
        TypeRef parent = listIterator.next();
        if (parentClassMapping.containsKey(parent)) {
          parent = parentClassMapping.get(parent);
          if (parent == null || parent.isPrimitive()) {
            continue;
          }
        }

        parentString.append(parent.getName());
        if (listIterator.hasNext()) {
          parentString.append(",");
        }
      }
      parentString.append(")");
    }

    content.append(nestString(String.format("class %s%s:\n", name, parentString), nestLevel));

    for (FieldSpec field : fields) {
      content.append(field.dump(nestLevel + 1, parentClassMapping));
      content.append("\n");
    }

    for (MethodSpec method : methods) {
      content.append(method.dump(nestLevel + 1, parentClassMapping));
      content.append("\n");
    }

    return content.toString();
  }
}
