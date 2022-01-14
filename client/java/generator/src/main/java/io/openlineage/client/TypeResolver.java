/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.client;


import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
import com.fasterxml.jackson.databind.ObjectMapper;

import io.openlineage.client.SchemaParser.AllOfType;
import io.openlineage.client.SchemaParser.ArrayType;
import io.openlineage.client.SchemaParser.Field;
import io.openlineage.client.SchemaParser.ObjectType;
import io.openlineage.client.SchemaParser.OneOfType;
import io.openlineage.client.SchemaParser.PrimitiveType;
import io.openlineage.client.SchemaParser.RefType;
import io.openlineage.client.SchemaParser.Type;
import io.openlineage.client.SchemaParser.TypeVisitor;

/**
 * Resolves the types in a Schema. (ref, etc)
 */
public class TypeResolver {

  private Map<String, ObjectResolvedType> types = new HashMap<>();
  private Set<String> referencedTypes = new HashSet<>();
  private Set<String> baseTypes = new HashSet<>();

  private Map<URL, ResolvedType> rootResolvedTypePerURL = new HashMap<URL, TypeResolver.ResolvedType>();

  public TypeResolver(Collection<URL> baseUrls) {
    super();
    for (final URL baseUrl : baseUrls) {

      String path = baseUrl.getPath();
      final String container = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.'));

      JsonNode rootSchema = readJson(baseUrl);
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
              container,
              asList(objectType),
              currentName,
              Collections.emptySet(),
              resolvedFields,
              objectType.hasAdditionalProperties(),
              visit(currentName + "Additional", objectType.getAdditionalPropertiesType()));
          String key = container + "." + objectResolvedType.getName();
          if (types.put(key, objectResolvedType) != null) {
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
          throw new UnsupportedOperationException("oneOf is not supported yet " + oneOfType);
        }

        @Override
        public ResolvedType visit(AllOfType allOfType) {
          List<Type> children = allOfType.getChildren();
          List<ObjectType> castChildren = new ArrayList<>(children.size());
          List<ResolvedField> combinedProperties = new ArrayList<>();
          boolean additionalProperties = false;
          ResolvedType additionalPropertiesType = null;
          Set<ObjectResolvedType> parents = new LinkedHashSet<>();
          for (Type child : children) {
            ObjectResolvedType resolvedChildType = (ObjectResolvedType) visit(child);
            List<ObjectType> objectTypes = resolvedChildType.getObjectTypes();
            if (!currentName.equals(resolvedChildType.getName())) {
              // base interface
              baseTypes.add(resolvedChildType.getName());
              parents.add(resolvedChildType);
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
          ObjectResolvedType objectResolvedType = new ObjectResolvedType(container, castChildren, currentName, parents, combinedProperties, additionalProperties, additionalPropertiesType);
          String key = container + "." + objectResolvedType.getName();
          types.put(key, objectResolvedType);
          return objectResolvedType;
        }

        @Override
        public ResolvedType visit(RefType refType) {
          String absolutePointer = refType.getPointer();
          int anchorIndex = absolutePointer.indexOf('#');
          String pointer = absolutePointer.substring(anchorIndex + 1);
          String base = absolutePointer.substring(0, anchorIndex);
          String typeName = titleCase(lastPart(pointer));
          final String refContainer;
          if (anchorIndex > 0) {
            String file = base.substring(base.lastIndexOf('/') + 1);
            refContainer = file.substring(0, file.lastIndexOf('.'));
          } else {
            refContainer = container;
          }
          String key = refContainer + "." + typeName;
          if (types.containsKey(key)) {
            return types.get(key);
          }
          if (anchorIndex > 0) {
            throw new RuntimeException("This ref should have been resolved already: " + refContainer + " " + refType.getPointer() + " => "+ key + " keys: " + types.keySet());
          }

          final JsonNode ref = rootSchema.at(pointer);
          if (ref.isMissingNode()) {
            throw new RuntimeException("ref " + pointer + " not found in " + rootSchema);
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

      this.rootResolvedTypePerURL.put(baseUrl, rootType.accept(visitor));
    }
  }

  private JsonNode readJson(URL baseUrl) {
    try {
      InputStream input;
      input = baseUrl.openStream();
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(input, JsonNode.class);
    } catch (IOException e) {
      throw new RuntimeException("Could not read json schema at " + baseUrl, e);
    }
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

  public ResolvedType getRootResolvedType(URL url) {
    return rootResolvedTypePerURL.get(url);
  }

  interface ResolvedType {
    <T> T accept(ResolvedTypeVisitor<T> visitor);
  }

  interface ResolvedTypeVisitor<T> {

    T visit(PrimitiveResolvedType primitiveType);

    T visit(ObjectResolvedType objectType);

    T visit(ArrayResolvedType arrayType);

    default T visit(ResolvedType type) {
      try {
        return type == null ? null : type.accept(this);
      } catch (RuntimeException e) {
        throw new RuntimeException("Exception while visiting " + type, e);
      }
    }

  }

  public static class DefaultResolvedTypeVisitor<T> implements ResolvedTypeVisitor<T> {

    public T visit(PrimitiveResolvedType primitiveType) { return null; }

    public T visit(ObjectResolvedType objectType) { return null; }

    public T visit(ArrayResolvedType arrayType) { return null; }

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

    @Override
    public String toString() {
      return "ResolvedField{name: " + field.getName() + ", type: " + type +  "}";
    }
  }


  static class ObjectResolvedType implements ResolvedType {
    private String container;
    private final List<ObjectType> objectTypes;
    private final String name;
    private final List<ResolvedField> properties;
    private boolean additionalProperties;
    private final ResolvedType additionalPropertiesType;
    private final Set<ObjectResolvedType> parents;

    public ObjectResolvedType(String container, List<ObjectType> objectTypes, String name, Set<ObjectResolvedType> parents, List<ResolvedField> properties, boolean additionalProperties, ResolvedType additionalPropertiesType) {
      super();
      this.container = container;
      this.objectTypes = objectTypes;
      this.name = name;
      this.parents = parents;
      this.properties = properties;
      this.additionalProperties = additionalProperties;
      this.additionalPropertiesType = additionalPropertiesType;
    }

    public String getContainer() {
      return container;
    }

    public List<ObjectType> getObjectTypes() {
      return objectTypes;
    }

    public String getName() {
      return name;
    }

    public Set<ObjectResolvedType> getParents() {
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

    public void setAdditionalProperties(boolean additionalProperties) {
      this.additionalProperties = additionalProperties;
    }

    @Override
    public <T> T accept(ResolvedTypeVisitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      return "ObjectResolvedType{" + name + "}";
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
