package io.openlineage.client;

import io.openlineage.client.python.ClassSpec;
import io.openlineage.client.python.DecoratorSpec;
import io.openlineage.client.python.FieldSpec;
import io.openlineage.client.python.PythonFile;
import io.openlineage.client.python.TypeRef;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PythonGenerator {
  private final TypeResolver typeResolver;
  private final Map<TypeRef, TypeRef> parentClassMapping;

  public void generate(PrintWriter printWriter) throws IOException {
    PythonFile file = new PythonFile();

    file.requirements.add(TypeRef.builder().name("attrs").build());
    file.requirements.add(TypeRef.builder().name("List").module("typing").build());

    Collection<TypeResolver.ObjectResolvedType> types = typeResolver.getTypes();

    for (TypeResolver.ObjectResolvedType type : types) {
      if (type.getName().length() == 0) {
        // we're generating types that have name (through ref)
        continue;
      }

      // Add enums
      for (TypeResolver.ResolvedField field : type.getProperties()) {
        if (field.getType() instanceof TypeResolver.EnumResolvedType) {
          ClassSpec enumClazz = buildEnum((TypeResolver.EnumResolvedType) field.getType());
          file.addClass(enumClazz);
        }
      }

      Optional<ClassSpec> clazz = buildClass(type);
      clazz.ifPresent(file::addClass);
    }
    printWriter.print(file.dump(0, parentClassMapping));
  }

  Optional<ClassSpec> buildClass(TypeResolver.ObjectResolvedType type) {
    ClassSpec.ClassSpecBuilder builder = ClassSpec.builder();
    builder.name(type.getName());

    Set<String> parentFields = new HashSet<>();
    Optional<ClassSpec> parentClass = Optional.empty();
    for (TypeResolver.ObjectResolvedType parent : type.getParents()) {
      // Prune "empty" types
      parentClass = buildClass(parent);
      if (!parentClass.isPresent()) {
        // Skip empty parent - look at it's parent
        continue;
      }

      builder.parent(TypeRef.builder().name(parent.getName()).isInternal(true).build());
      for (TypeResolver.ResolvedField field : parent.getProperties()) {
        parentFields.add(field.getName());
      }
    }

    for (TypeResolver.ResolvedField field : type.getProperties()) {
      FieldSpec.FieldSpecBuilder fieldSpecBuilder = FieldSpec.builder();
      TypeRef typeRef = getTypeRef(field.getType());

      if (parentFields.contains(field.getName())) {
        System.out.printf("%s contains %s\n", parentFields, field.getName());
        continue;
      }

      if (!typeRef.isPrimitive() && !typeRef.isInternal()) {
        TypeRef arrayRef = typeRef.getArrayRef();
        if (arrayRef != null) {
          if (!arrayRef.isPrimitive() && !arrayRef.isInternal()) {
            builder.requirement(arrayRef);
          }
        } else {
          builder.requirement(typeRef);
        }
      }
      fieldSpecBuilder.name(field.getName());
      fieldSpecBuilder.type(typeRef);
      builder.field(fieldSpecBuilder.build());
    }

    builder.decorator(DecoratorSpec.builder().name("attrs.define").build());

    ClassSpec clazz = builder.build();
    if (clazz.fields.isEmpty()) {
      //      System.out.printf("%s %s\n", clazz.getName(),
      // Arrays.toString(clazz.getParents().toArray()));
      if (parentClass.isPresent()) {
        parentClassMapping.put(clazz.getTypeRef(), parentClass.get().getTypeRef());
        return parentClass;
      }
      parentClassMapping.put(clazz.getTypeRef(), null);
      return Optional.empty();
    }
    return Optional.of(clazz);
  }

  ClassSpec buildEnum(TypeResolver.EnumResolvedType type) {
    ClassSpec.ClassSpecBuilder builder = ClassSpec.builder();
    builder.name(type.getName());
    builder.isEnum(true);
    builder.parent(TypeRef.builder().name("Enum").module("enum").build());
    builder.requirement(TypeRef.builder().name("Enum").module("enum").build());

    for (String value : type.getValues()) {
      FieldSpec.FieldSpecBuilder fieldSpecBuilder = FieldSpec.builder();
      fieldSpecBuilder.name(value);
      fieldSpecBuilder.defaultValue(String.format("\"%s\"", value.toUpperCase()));
      builder.field(fieldSpecBuilder.build());
    }
    return builder.build();
  }

  public TypeRef getTypeRef(TypeResolver.ResolvedType type) {
    return type.accept(
        new TypeResolver.ResolvedTypeVisitor<TypeRef>() {
          @Override
          public TypeRef visit(TypeResolver.PrimitiveResolvedType primitiveType) {
            if (primitiveType.getName().equals("integer")) {
              return TypeRef.INT;
            } else if (primitiveType.getName().equals("number")) {
              return TypeRef.FLOAT;
            } else if (primitiveType.getName().equals("boolean")) {
              return TypeRef.BOOL;
            } else if (primitiveType.getName().equals("string")) {
              if (primitiveType.getFormat() != null) {
                String format = primitiveType.getFormat();
                switch (format) {
                  case "uri":
                  case "uuid":
                    return TypeRef.STR; // todo?
                  case "date-time":
                    return TypeRef.DATETIME; // real classname? move to typedef?
                  default:
                    throw new RuntimeException("Unknown format: " + primitiveType.getFormat());
                }
              }
              return TypeRef.STR;
            }
            throw new RuntimeException("Unknown primitive: " + primitiveType.getName());
          }

          @Override
          public TypeRef visit(TypeResolver.ObjectResolvedType objectType) {
            return TypeRef.builder()
                .name(objectType.getName())
                .isPrimitive(false)
                .isInternal(true)
                .build();
          }

          @Override
          public TypeRef visit(TypeResolver.ArrayResolvedType arrayType) {
            var internal = visit(arrayType.getItems());
            return TypeRef.builder()
                .name(String.format("List[%s]", internal.getName()))
                .arrayRef(internal)
                .isPrimitive(false)
                .isInternal(false)
                .build();
          }

          @Override
          public TypeRef visit(TypeResolver.EnumResolvedType enumType) {
            return TypeRef.builder().name(enumType.getName()).isInternal(false).build();
          }
        });
  }
}
