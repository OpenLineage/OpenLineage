package io.openlineage.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class TypeResolver {
  private JsonNode rootSchema;
  private Map<String, ObjectType> types = new HashMap<>();
  private Set<String> referencedTypes = new HashSet<>();
  private Set<String> baseTypes = new HashSet<>();

  public TypeResolver(JsonNode rootSchema) {
    super();
    this.rootSchema = rootSchema;
    JsonNode jsonNode = rootSchema.get("oneOf");
    if (jsonNode.isArray()) {
      for (final JsonNode type : jsonNode) {
        resolveType("RunEvent", null, type);
      }
    }
  }

  interface TypeVisitor<T> {

    T visit(PrimitiveType primitiveType);

    T visit(ObjectType objectType);

    T visit(ArrayType arrayType);

  }

  interface Type {

    <T> T accept(TypeVisitor<T> visitor);
    default boolean isObject() { return false; };
    default ObjectType asObject() { return (ObjectType) this; };
  }

  static class PrimitiveType implements Type {
    private String name;

    public PrimitiveType(String name) {
      super();
      this.name = name;
    }

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      return "PrimitiveType [name=" + name + "]";
    }

    public String getName() {
      return name;
    }
  }

  public static class Field {
    private String name;
    private Type type;
    private  String description;

    public Field(String name, Type type, String description) {
      super();
      this.name = name;
      this.type = type;
      this.description = description;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
      }

    public String getDescription() {
      return description;
    }

  }

  static class ObjectType implements Type {
    private String name;
    private List<Field> properties;
    private Set<String> parents;

    public ObjectType(String name, List<Field> properties, Set<String> parents) {
      super();
      this.name = name;
      this.properties = properties;
      this.parents = parents;
    }
    public boolean isObject() { return true; };
    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

    public String getName() {
      return name;
    }

    public List<Field> getProperties() {
      return properties;
    }

    public Set<String> getParents() {
      return parents;
    }

    @Override
    public String toString() {
      return "ObjectType [name=" + name + ", properties=" + properties + "]";
    }

  }

  static class ArrayType implements Type {
    private Type items;

    public ArrayType(Type items) {
      super();
      this.items = items;
    }

    public Type getItems() {
      return items;
    }

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }
    @Override
    public String toString() {
      return "ArrayType [items=" + items + "]";
    }

  }

  public static class Schema {
    String name;
    JsonNode schema;
    boolean isPrimitive;
    public Schema(String name, JsonNode schema, boolean isPrimitive) {
      super();
      this.name = name;
      this.schema = schema;
    }
    public String getName() {
      return name;
    }
    public JsonNode getSchema() {
      return schema;
    }
    @Override
    public String toString() {
      return name + " " + schema;
    }
    public String getType() {
      return schema.get("type").asText();
    }
    public boolean has(String fieldName) {
      return schema.has(fieldName);
    }
    public JsonNode get(String fieldName) {
      return schema.get(fieldName);
    }

    public boolean isObject() {
      return schema.has("type") && schema.get("type").asText().equals("object");
    }

    public Schema rename(String newName) {
      return new Schema(newName, schema, isPrimitive);
    }

  }

  private ObjectType resolveObjectType(String parentName, String fieldName, Schema schema) {
    //  comment("resolveObjectType " + parentName + "." + fieldName + " " + schema);
    if (schema.getName() != null && types.containsKey(schema.getName())) {
      return types.get(schema.getName());
    }
    String type = schema.getType();
    Set<String> parents = new LinkedHashSet<String>();
    List<Field> fields = new ArrayList<Field>();
    if (type.equals("object")) {
      if (schema.has("allOf")) {
        JsonNode allOf = schema.get("allOf");
        for (JsonNode oneOf : allOf) {
          Type oneType = resolveType(schema.getName(), null, oneOf);
          String baseName = oneType.asObject().name;
          if (!baseName.contentEquals(schema.getName())) {
            baseTypes.add(baseName);
            parents.add(baseName);
          }
          fields.addAll(oneType.asObject().properties);
        }
      } else  if (schema.has("properties")) {
        fields.addAll(resolveFields(schema));
      } else {
        throw new RuntimeException("Unknown type " + schema);
      }
    }
    ObjectType result = new ObjectType(schema.getName(), fields, parents);
    if (schema.getName() != null) {
      types.put(schema.getName(), result);
    }
    return result;
  }

  private List<Field> resolveFields(Schema schema) {
    //  comment("resolveFields " + schema);
    List<Field> result = new ArrayList<Field>();
    JsonNode properties = schema.get("properties");

    for (Iterator<Entry<String, JsonNode>> fields = properties.fields(); fields.hasNext(); ) {
      Entry<String, JsonNode> field = fields.next();
      Type fieldType = resolveType(schema.getName(), field.getKey(), field.getValue());
      referencedTypes.add(fieldType.accept(new TypeVisitor<String>() {
        @Override
        public String visit(PrimitiveType primitiveType) {
          return primitiveType.name;
        }

        @Override
        public String visit(ObjectType objectType) {
          return objectType.name;
        }

        @Override
        public String visit(ArrayType arrayType) {
          return arrayType.items.accept(this);
        }

      }));
      String description = field.getValue().has("description") ? field.getValue().get("description").asText() : null;
      result.add(new Field(field.getKey(), fieldType, description));
    }
    return result;
  }

  private Type resolveType(String parentName, String fieldName, JsonNode typeNode) {
    //  comment("resolveType " + parentName + "." + fieldName + " " + typeNode);
    try {
      if (typeNode.has("$ref")) {
        String pointer = typeNode.get("$ref").asText().substring(1);
        JsonNode ref = rootSchema.at(pointer);
        String name = lastPart(pointer);
        Schema schema = new Schema(name, ref, false);
        return resolveObjectType(parentName, fieldName, schema);
      } else if (typeNode.has("type")) {
        String fieldType = typeNode.get("type").asText();
        if (fieldType.equals("string")) {
          return new PrimitiveType("String");
        } else if (fieldType.equals("object")) {
          String name = fieldName == null ? parentName : parentName + titleCase(fieldName);
          Schema schema = new Schema(name, typeNode, false);
          return resolveObjectType(parentName, fieldName, schema);
        } else if (fieldType.equals("array")) {
          Type itemsType = resolveType(parentName, fieldName, typeNode.get("items"));
          return new ArrayType(itemsType);
        }
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("can't resolve type " + parentName+"."+fieldName + " = " + typeNode, e);
    }
    throw new RuntimeException("unknown schema " + typeNode);
  }

  public static String titleCase(String name) {
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }

  private String lastPart(String pointer) {
    int i = pointer.lastIndexOf("/");
    return pointer.substring(i + 1);
  }

  public Collection<ObjectType> getTypes() {
    return types.values();
  }

  public Set<String> getBaseTypes() {
    return baseTypes;
  }

}