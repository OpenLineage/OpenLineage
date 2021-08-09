package io.openlineage.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;

public class SchemaParser {

  public Type parse(JsonNode typeJson) {
    try {
      if (typeJson.has("oneOf")) {
        return new OneOfType(parseChildren(typeJson.get("oneOf")));
      } else if (typeJson.has("allOf")) {
        return new AllOfType(parseChildren(typeJson.get("allOf")));
      } else if (typeJson.has("$ref")) {
        String pointer = typeJson.get("$ref").asText();
        return new RefType(pointer);
      } else if (typeJson.has("type")) {
        String typeName = typeJson.get("type").asText();
        if (typeName.equals("string") || typeName.equals("integer") || typeName.equals("number") || typeName.equals("boolean")) {
          return new PrimitiveType(typeName, typeJson.has("format") ? typeJson.get("format").asText() : null);
        } else if (typeName.equals("object") && (typeJson.has("properties") || typeJson.has("additionalProperties") || typeJson.has("patternProperties"))) {
          List<Field> fields = new ArrayList<Field>();
          boolean hasAdditionalProperties = false;
          Type additionalPropertiesType = null;
          if (typeJson.has("properties")) {
            JsonNode properties = typeJson.get("properties");
            for (Iterator<Entry<String, JsonNode>> fieldsJson = properties.fields(); fieldsJson.hasNext(); ) {
              Entry<String, JsonNode> field = fieldsJson.next();
              Type fieldType = parse(field.getValue());
              String description = field.getValue().has("description") ? field.getValue().get("description").asText() : null;
              fields.add(new Field(field.getKey(), fieldType, description));
            }
          }
          if (typeJson.has("additionalProperties")) {
            hasAdditionalProperties = true;
            JsonNode additionalProperties = typeJson.get("additionalProperties");
            if (additionalProperties.isObject()) {
              additionalPropertiesType = parse(additionalProperties);
            }
          } else if (typeJson.has("patternProperties")) {
            hasAdditionalProperties = true;
            JsonNode patternProperties = typeJson.get("patternProperties");
            if (patternProperties.isObject()) {
              Iterator<Entry<String, JsonNode>> patternFields = patternProperties.fields();
              Entry<String, JsonNode> patternField = patternFields.hasNext() ? patternFields.next() : null;
              if (patternField == null || patternFields.hasNext() || !patternField.getValue().isObject()) {
                throw new RuntimeException("can't parse type " + patternProperties);
              }
              additionalPropertiesType = parse(patternField.getValue());
            }
          }
          return new ObjectType(fields, hasAdditionalProperties, additionalPropertiesType);
        } else if (typeName.equals("array")) {
          Type itemsType = parse(typeJson.get("items"));
          return new ArrayType(itemsType);
        }
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("can't parse type " + typeJson, e);
    }
    throw new RuntimeException("Invalid schema, unknown type: " + typeJson);
  }

  private List<Type> parseChildren(JsonNode children) {
    List<Type> types = new ArrayList<SchemaParser.Type>();
    if (!children.isArray()) {
      throw new RuntimeException("Invalid schema, should be array: " + children);
    }
    for (JsonNode child : children) {
      types.add(parse(child));
    }
    return types;
  }

  interface TypeVisitor<T> {

    T visit(PrimitiveType primitiveType);

    T visit(ObjectType objectType);

    T visit(ArrayType arrayType);

    T visit(OneOfType oneOfType);

    T visit(AllOfType allOfType);

    T visit(RefType refType);

    default T visit(Type type) {
      return type == null ? null : type.accept(this);
    }

  }

  interface Type {

    <T> T accept(TypeVisitor<T> visitor);
    default boolean isObject() { return false; };
    default ObjectType asObject() { return (ObjectType) this; };
  }


  static class RefType implements Type {

    private final String pointer;

    public RefType(String pointer) {
      this.pointer = pointer;
    }

    public String getPointer() {
      return pointer;
    }

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

  }

  static class OneOfType implements Type {

    private final List<Type> types;

    public OneOfType(List<Type> types) {
      this.types = types;
    }

    public List<Type> getTypes() {
      return types;
    }

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

  }

  static class AllOfType implements Type {

    private final List<Type> children;

    public AllOfType(List<Type> children) {
      this.children = children;
    }

    public List<Type> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

  }

  static class PrimitiveType implements Type {
    private final String name;
    private final String format;

    protected PrimitiveType(PrimitiveType pt) {
      super();
      this.name = pt.name;
      this.format = pt.format;
    }

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
    private final List<Field> properties;
    private final boolean additionalProperties;
    private final Type additionalPropertiesType;

    public ObjectType(List<Field> properties, boolean additionalProperties, Type additionalPropertiesType) {
      super();
      this.properties = properties;
      this.additionalProperties = additionalProperties;
      this.additionalPropertiesType = additionalPropertiesType;
    }

    public boolean isObject() { return true; };

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

    public List<Field> getProperties() {
      return properties;
    }

    public boolean hasAdditionalProperties() {
      return additionalProperties;
    }

    public Type getAdditionalPropertiesType() {
      return additionalPropertiesType;
    }

    @Override
    public String toString() {
      return "ObjectType [properties=" + properties + "]";
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

}
