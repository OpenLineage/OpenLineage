package io.openlineage.client;


import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import io.openlineage.client.SchemaParser.AllOfType;
import io.openlineage.client.SchemaParser.ArrayType;
import io.openlineage.client.SchemaParser.Field;
import io.openlineage.client.SchemaParser.ObjectType;
import io.openlineage.client.SchemaParser.OneOfType;
import io.openlineage.client.SchemaParser.PrimitiveType;
import io.openlineage.client.SchemaParser.RefType;
import io.openlineage.client.SchemaParser.Type;
import io.openlineage.client.SchemaParser.TypeVisitor;

public class TypeResolver {

  private Map<String, ObjectResolvedType> types = new HashMap<>();
  private Set<String> referencedTypes = new HashSet<>();
  private Set<String> baseTypes = new HashSet<>();

  private ResolvedType rootResolvedType;

  public TypeResolver(JsonNode rootSchema) {
    super();
    SchemaParser parser = new SchemaParser();
    Type rootType = parser.parse(rootSchema);

    TypeVisitor<ResolvedType> visitor = new TypeVisitor<ResolvedType>(){

      String currentName = "";

      @Override
      public ResolvedType visit(PrimitiveType primitiveType) {
        return new PrimitiveResolvedType(primitiveType);
      }

      @Override
      public ResolvedType visit(ObjectType objectType) {
        List<Field> properties = objectType.getProperties();
        List<ResolvedField> resolvedFields = new ArrayList<>(properties.size());
        resolvedFields.addAll(resolveFields(properties));
        ObjectResolvedType objectResolvedType = new ObjectResolvedType(
            asList(objectType),
            currentName,
            Collections.emptySet(),
            resolvedFields,
            objectType.hasAdditionalProperties(),
            visit(currentName + "Additional", objectType.getAdditionalPropertiesType()));
        if (types.put(objectResolvedType.getName(), objectResolvedType) != null) {
          throw new RuntimeException("Duplicated type: " + objectResolvedType.getName());
        };
        return objectResolvedType;
      }

      private List<ResolvedField> resolveFields(List<Field> properties) {
        List<ResolvedField> resolvedFields = new ArrayList<>(properties.size());
        String previousCurrentName = currentName;
        for (Field property : properties) {
          currentName = previousCurrentName + titleCase(property.getName());
          ResolvedField resolvedField = new ResolvedField(property, visit(property.getType()));
          resolvedFields.add(resolvedField);
          referencedTypes.add(resolvedField.getType().accept(new ResolvedTypeVisitor<String>() {
            @Override
            public String visit(PrimitiveResolvedType primitiveType) {
              return primitiveType.getName();
            }

            @Override
            public String visit(ObjectResolvedType objectType) {
              return objectType.getName();
            }

            @Override
            public String visit(ArrayResolvedType arrayType) {
              return visit(arrayType.items);
            }

          }));
        }
        currentName = previousCurrentName;
        return resolvedFields;
      }

      @Override
      public ResolvedType visit(ArrayType arrayType) {
        return new ArrayResolvedType(arrayType, visit(arrayType.getItems()));
      }

      @Override
      public ResolvedType visit(OneOfType oneOfType) {
        List<Type> types = oneOfType.getTypes();
        if (types.size() != 1) {
          throw new UnsupportedOperationException("Only oneOf of size 1 accepted for now: " + types);
        }
        return visit(types.get(0));
      }

      @Override
      public ResolvedType visit(AllOfType allOfType) {
        List<Type> children = allOfType.getChildren();
        List<ObjectType> castChildren = new ArrayList<>(children.size());
        List<ResolvedField> combinedProperties = new ArrayList<>();
        boolean additionalProperties = false;
        ResolvedType additionalPropertiesType = null;
        Set<String> parents = new LinkedHashSet<String>();
        for (Type child : children) {
          ObjectResolvedType resolvedChildType = (ObjectResolvedType) visit(child);
          List<ObjectType> objectTypes = resolvedChildType.getObjectTypes();
          if (!currentName.equals(resolvedChildType.getName())) {
            // base interface
            baseTypes.add(resolvedChildType.getName());
            parents.add(resolvedChildType.getName());
          }
          castChildren.addAll(objectTypes);
          combinedProperties.addAll(resolvedChildType.getProperties());
          if (resolvedChildType.hasAdditionalProperties()) {
            additionalProperties = true;
            if (resolvedChildType.getAdditionalPropertiesType() != null) {
              if (additionalPropertiesType == null) {
                additionalPropertiesType = resolvedChildType.getAdditionalPropertiesType();
              } else {
                if (!additionalPropertiesType.equals(resolvedChildType.getAdditionalPropertiesType())) {
                  throw new UnsupportedOperationException("can not combine different additionalProperties types in allOf " + children);
                }
              }
            }
          }
        }
        ObjectResolvedType objectResolvedType = new ObjectResolvedType(castChildren, currentName, parents, combinedProperties, additionalProperties, additionalPropertiesType);
        types.put(objectResolvedType.getName(), objectResolvedType);
        return objectResolvedType;
      }

      @Override
      public ResolvedType visit(RefType refType) {
        String absolutePointer = refType.getPointer();
        if (!absolutePointer.startsWith("#")) {
          throw new RuntimeException("For now we support only types in the same file (starting with #): " + absolutePointer);
        }
        String pointer = absolutePointer.substring(1);
        JsonNode ref = rootSchema.at(pointer);
        String typeName = lastPart(pointer);
        if (types.containsKey(typeName)) {
          return types.get(typeName);
        }
        return visit(typeName, parser.parse(ref));
      }

      private String lastPart(String pointer) {
        int i = pointer.lastIndexOf("/");
        return pointer.substring(i + 1);
      }

      ResolvedType visit(String name, Type type) {
        String previousCurrentName = currentName;
        currentName = name;
        try {
          return visit(type);
        } finally {
          currentName = previousCurrentName;
        }
      }

    };

    this.rootResolvedType = rootType.accept(visitor);
  }

  public static String titleCase(String name) {
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  public Collection<ObjectResolvedType> getTypes() {
    return types.values();
  }

  public Set<String> getBaseTypes() {
    return baseTypes;
  }

  public ResolvedType getRootResolvedType() {
    return rootResolvedType;
  }

  interface ResolvedType {
    <T> T accept(ResolvedTypeVisitor<T> visitor);
  }

  interface ResolvedTypeVisitor<T> {

    T visit(PrimitiveResolvedType primitiveType);

    T visit(ObjectResolvedType objectType);

    T visit(ArrayResolvedType arrayType);

    default T visit(ResolvedType type) {
      return type == null ? null : type.accept(this);
    }

  }

  static class PrimitiveResolvedType implements ResolvedType {

    private PrimitiveType primitiveType;

    public PrimitiveResolvedType(PrimitiveType primitiveType) {
      this.primitiveType = primitiveType;
    }

    public String getName() {
      return primitiveType.getName();
    }

    public String getFormat() {
      return primitiveType.getFormat();
    }

    @Override
    public <T> T accept(ResolvedTypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

  }

  public static class ResolvedField {
    private final Field field;
    private final ResolvedType type;

    public ResolvedField(Field field, ResolvedType type) {
      super();
      this.field = field;
      this.type = type;
    }

    public String getName() {
      return field.getName();
    }

    public String getDescription() {
      return field.getDescription();
    }

    public ResolvedType getType() {
      return type;
    }

  }

  static class ObjectResolvedType implements ResolvedType {
    private final List<ObjectType> objectTypes;
    private final String name;
    private final List<ResolvedField> properties;
    private final boolean additionalProperties;
    private final ResolvedType additionalPropertiesType;
    private final Set<String> parents;

    public ObjectResolvedType(List<ObjectType> objectTypes, String name, Set<String> parents, List<ResolvedField> properties, boolean additionalProperties, ResolvedType additionalPropertiesType) {
      super();
      this.objectTypes = objectTypes;
      this.name = name;
      this.parents = parents;
      this.properties = properties;
      this.additionalProperties = additionalProperties;
      this.additionalPropertiesType = additionalPropertiesType;
    }

    public List<ObjectType> getObjectTypes() {
      return objectTypes;
    }

    public String getName() {
      return name;
    }

    public Set<String> getParents() {
      return parents;
    }

    public List<ResolvedField> getProperties() {
      return properties;
    }

    public boolean hasAdditionalProperties() {
      return additionalProperties;
    }

    public ResolvedType getAdditionalPropertiesType() {
      return additionalPropertiesType;
    }

    @Override
    public <T> T accept(ResolvedTypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

  }

  static class ArrayResolvedType implements ResolvedType {

    private final ArrayType arrayType;
    private final ResolvedType items;

    public ArrayResolvedType(ArrayType arrayType, ResolvedType items) {
      this.arrayType = arrayType;
      this.items = items;
    }

    public ResolvedType getItems() {
      return items;
    }

    @Override
    public <T> T accept(ResolvedTypeVisitor<T> visitor) {
      return visitor.visit(this);
    }
  }

}
