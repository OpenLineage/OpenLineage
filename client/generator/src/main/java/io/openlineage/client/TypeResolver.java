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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class TypeResolver {
  private static final Logger logger = LoggerFactory.getLogger(TypeResolver.class);
  private JsonNode rootSchema;
  private Map<String, ObjectType> types = new HashMap<>();
  private Set<String> referencedTypes = new HashSet<>();
  private Set<String> baseTypes = new HashSet<>();
  private List<Type> rootTypes = new ArrayList<>();

  public TypeResolver(JsonNode rootSchema) {
    super();
    this.rootSchema = rootSchema;
    JsonNode jsonNode = rootSchema.get("oneOf");
    if (jsonNode.isArray()) {
      for (final JsonNode type : jsonNode) {
        rootTypes.add(resolveType(new Schema("RunEvent", type)));
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
    private final String name;
    private final String format;

    public PrimitiveType(String name, String format) {
      super();
      this.name = name;
      this.format = format;
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

    public String getFormat() {
      return format;
    }
  }

  public static class Field {
    private String name;
    private Type type;
    private String description;

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
    private final String name;
    private final List<Field> properties;
    private final Set<String> parents;
    private final boolean hasAdditionalProperties;
    private final Type additionalPropertiesType;

    public ObjectType(String name, List<Field> properties, Set<String> parents, boolean hasAdditionalProperties, Type additionalPropertiesType) {
      super();
      this.name = name;
      this.properties = properties;
      this.parents = parents;
      this.additionalPropertiesType = additionalPropertiesType;
      this.hasAdditionalProperties = hasAdditionalProperties;
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

    public boolean isHasAdditionalProperties() {
      return hasAdditionalProperties;
    }

    public Type getAdditionalPropertiesType() {
      return additionalPropertiesType;
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
    public Schema(String name, JsonNode schema) {
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
      return new Schema(newName, schema);
    }

  }

  private ObjectType resolveObjectType(Schema schema) {
    logger.debug("resolveObjectType " + schema);
    try {
      if (schema.getName() != null && types.containsKey(schema.getName())) {
        return types.get(schema.getName());
      }
      String type = schema.getType();
      Set<String> parents = new LinkedHashSet<String>();
      List<Field> fields = new ArrayList<Field>();
      boolean hasAdditionalProperties = false;
      Type additionalPropertiesType = null;
      if (type.equals("object")) {
        if (schema.has("allOf")) {
          JsonNode allOf = schema.get("allOf");
          for (JsonNode node : allOf) {
            ObjectType oneType = resolveType(new Schema(schema.getName(), node)).asObject();
            String baseName = oneType.asObject().name;
            if (!baseName.contentEquals(schema.getName())) {
              // base interface
              baseTypes.add(baseName);
              parents.add(baseName);
            } else {
              // actual type definition, possibly includes additional properties
              if (oneType.hasAdditionalProperties) {
                hasAdditionalProperties = true;
                additionalPropertiesType = oneType.additionalPropertiesType;
              }
            }
            fields.addAll(oneType.properties);
          }
        } else  if (schema.has("properties") || schema.has("additionalProperties")) {
          fields.addAll(resolveFields(schema));
          if (schema.has("additionalProperties")) {
            hasAdditionalProperties = true;
            JsonNode additionalProperties = schema.get("additionalProperties");
            if (additionalProperties.isObject()) {
              additionalPropertiesType = resolveType(new Schema(schema.getName() + "_additionalProperties", additionalProperties));
            }
          }
        } else {
          throw new RuntimeException("Unknown object type " + schema);
        }
      } else {
        throw new RuntimeException("This should be and object: " + schema);
      }
      ObjectType result = new ObjectType(schema.getName(), fields, parents, hasAdditionalProperties, additionalPropertiesType);
      if (schema.getName() != null) {
        types.put(schema.getName(), result);
      }
      return result;
    } catch (RuntimeException e) {
      throw new RuntimeException("can't resolve object type " + schema, e);
    }
  }

  private List<Field> resolveFields(Schema schema) {
    logger.debug("resolveFields " + schema);
    List<Field> result = new ArrayList<Field>();
    if (!schema.has("properties")) {
      return result;
    }
    JsonNode properties = schema.get("properties");

    for (Iterator<Entry<String, JsonNode>> fields = properties.fields(); fields.hasNext(); ) {
      Entry<String, JsonNode> field = fields.next();
      Type fieldType = resolveType(new Schema(schema.getName()+ titleCase(field.getKey()), field.getValue()));
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

  private Type resolveType(Schema typeNode) {
    logger.debug("resolveType " + typeNode);
    try {
      if (typeNode.has("$ref")) {
        String absolutePointer = typeNode.get("$ref").asText();
        if (!absolutePointer.startsWith("#")) {
          throw new RuntimeException("For now we support only types in the same file (starting with #): " + absolutePointer);
        }
        String pointer = absolutePointer.substring(1);
        JsonNode ref = rootSchema.at(pointer);
        String name = lastPart(pointer);
        Schema schema = new Schema(name, ref);
        return resolveObjectType(schema);
      } else if (typeNode.has("type")) {
        String fieldType = typeNode.get("type").asText();
        String fieldFormat = typeNode.has("format") ? typeNode.get("format").asText() : null;
        if (fieldType.equals("string")) {
          return new PrimitiveType(fieldType, fieldFormat);
        } else if (fieldType.equals("object")) {
          return resolveObjectType(typeNode);
        } else if (fieldType.equals("array")) {
          Type itemsType = resolveType(new Schema(typeNode.getName(), typeNode.getSchema().get("items")));
          return new ArrayType(itemsType);
        }
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("can't resolve type " + typeNode, e);
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

  public List<Type> getRootTypes() {
    return rootTypes;
  }

}