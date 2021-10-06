package io.openlineage.client;

import static io.openlineage.client.TypeResolver.titleCase;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import io.openlineage.client.TypeResolver.ArrayResolvedType;
import io.openlineage.client.TypeResolver.ObjectResolvedType;
import io.openlineage.client.TypeResolver.PrimitiveResolvedType;
import io.openlineage.client.TypeResolver.ResolvedField;
import io.openlineage.client.TypeResolver.ResolvedType;
import io.openlineage.client.TypeResolver.ResolvedTypeVisitor;


/**
 * Generates a JavaClass with all the types as inner classes
 */
public class JavaPoetGenerator {

  private final TypeResolver typeResolver;
  private final Map<String, URL> containerToID;
  private final String containerPackage;
  private final String containerClassName;
  private final boolean server;
  private final String containerClass;

  public JavaPoetGenerator(TypeResolver typeResolver, String containerPackage, String containerClassName, boolean server, Map<String, URL> containerToID) {
    this.typeResolver = typeResolver;
    this.containerPackage = containerPackage;
    this.containerClassName = containerClassName;
    this.server = server;
    if (containerToID == null) {
      throw new RuntimeException("missing baseURL");
    }
    this.containerToID = containerToID;
    this.containerClass = containerPackage + "." + containerClassName;
  }

  public void generate(PrintWriter printWriter) throws IOException {

    TypeSpec.Builder containerTypeBuilder = TypeSpec.classBuilder(containerClassName)
        .addModifiers(PUBLIC, FINAL);

    if (!server) {
      containerTypeBuilder.addField(FieldSpec.builder(ClassName.get(URI.class), "producer", PRIVATE, FINAL).build());
      containerTypeBuilder.addMethod(MethodSpec.constructorBuilder()
          .addModifiers(PUBLIC)
          .addParameter(
              ParameterSpec.builder(ClassName.get(URI.class), "producer").build()
              )
          .addCode("this.producer = producer;\n")
          .build());
    }

    generateTypes(containerTypeBuilder);
    TypeSpec openLineage = containerTypeBuilder.build();

    JavaFile javaFile = JavaFile.builder(containerPackage, openLineage)
        .build();

    javaFile.writeTo(printWriter);
  }

  private void generateTypes(TypeSpec.Builder containerTypeBuilder) {
    Collection<ObjectResolvedType> types = typeResolver.getTypes();
    for (ObjectResolvedType type : types) {
      if (type.getName().length() == 0) {
        // we're generating types that have name (through ref)
        continue;
      }
      if (typeResolver.getBaseTypes().contains(type.getName())) { // interfaces
        generateInterface(containerTypeBuilder, type);
      } else { // concrete types
        // We generate:
        // A data class
        // A factory method
        // A builder class

        TypeSpec builderClassSpec = builderClass(type);
        TypeSpec modelClassSpec = modelClass(type);

        containerTypeBuilder.addType(modelClassSpec);

        if (! server) {
          containerTypeBuilder.addMethod(factoryModelMethodUnderContainer(type));
          containerTypeBuilder.addMethod(MethodSpec.methodBuilder("new" + type.getName() + "Builder")
              .addModifiers(PUBLIC)
              .returns(ClassName.get(containerClass, type.getName() + "Builder"))
              .addCode("return new $N();", type.getName() + "Builder")
              .build());
          containerTypeBuilder.addType(builderClassSpec);
        }
      }
    }
  }

  private MethodSpec modelConstructor(ObjectResolvedType type) {
    Builder constructor = MethodSpec.constructorBuilder();
    if (type.getName().equals("CustomFacet") || server) {
      constructor.addModifiers(PUBLIC);
    } else {
      constructor.addModifiers(PRIVATE);
    }

    constructor.addAnnotation(JsonCreator.class);

    for (ResolvedField f : type.getProperties()) {
      if (isASchemaUrlField(f)) {
        String schemaURL = containerToID.get(type.getContainer()) + "#/$defs/" + type.getName();
        constructor.addCode("this.$N = URI.create($S);\n", f.getName(), schemaURL);
      } else {
        constructor.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
        constructor.addParameter(
            ParameterSpec.builder(getTypeName(f.getType()), f.getName())
                .addAnnotation(AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", f.getName()).build())
                .build());
        constructor.addCode("this.$N = $N;\n", f.getName(), f.getName());
      }
    }
    if (type.hasAdditionalProperties()) {
      constructor.addCode(CodeBlock.builder().addStatement("this.$N = new $T<>()", "additionalProperties", LinkedHashMap.class).build());
    }
    return constructor.build();
  }

  private TypeSpec modelClass(ObjectResolvedType type) {
    TypeSpec.Builder modelClassBuilder = TypeSpec.classBuilder(type.getName())
        .addModifiers(STATIC, PUBLIC);
    if (!server) {
      modelClassBuilder.addAnnotation(AnnotationSpec.builder(JsonDeserialize.class)
          .addMember("as", CodeBlock.of(type.getName() + ".class"))
          .build());
    }
    for (ObjectResolvedType parent : type.getParents()) {
      modelClassBuilder.addSuperinterface(ClassName.get(containerPackage, parent.getContainer(), parent.getName()));
    }
    //adds possibility to extend CustomFacet
    if (!type.getName().equals("CustomFacet")) {
      modelClassBuilder.addModifiers(FINAL);
    }

    com.squareup.javapoet.AnnotationSpec.Builder jsonPropertyOrder = AnnotationSpec.builder(JsonPropertyOrder.class);
    for (ResolvedField f : type.getProperties()) {
      modelClassBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE, FINAL);
      MethodSpec getter = getter(f)
          .addModifiers(PUBLIC)
          .addCode("return $N;", f.getName())
          .build();
      modelClassBuilder.addMethod(getter);

      jsonPropertyOrder.addMember("value", "$S", f.getName());

    }

    if (type.hasAdditionalProperties()) {
      String fieldName = "additionalProperties";
      TypeName additionalPropertiesValueType = type.getAdditionalPropertiesType() == null ? ClassName.get(Object.class) : getTypeName(type.getAdditionalPropertiesType());
      TypeName additionalPropertiesType = ParameterizedTypeName.get(ClassName.get(Map.class), ClassName.get(String.class), additionalPropertiesValueType);
      modelClassBuilder.addMethod(MethodSpec
          .methodBuilder("get" + titleCase(fieldName))
          .addJavadoc("@return additional properties")
          .returns(additionalPropertiesType)
          .addModifiers(PUBLIC)
          .addCode("return $N;", fieldName)
          .addAnnotation(AnnotationSpec.builder(JsonAnyGetter.class).build())
          .build());
      modelClassBuilder.addField(
          FieldSpec.builder(additionalPropertiesType, fieldName, PRIVATE, FINAL)
              .addAnnotation(JsonAnySetter.class)
              .build());
    }
    modelClassBuilder.addAnnotation(jsonPropertyOrder.build());
    MethodSpec modelConstructor = modelConstructor(type);
    modelClassBuilder.addMethod(modelConstructor);
    return modelClassBuilder.build();
  }

  private TypeSpec builderClass(ObjectResolvedType type) {
    TypeSpec.Builder builderClassBuilder = TypeSpec.classBuilder(type.getName() + "Builder")
        .addModifiers(PUBLIC, FINAL);

    boolean producerFiledExist = type.getProperties().stream()
        .anyMatch(this::isAProducerField);
    if (!producerFiledExist) builderClassBuilder.addModifiers(STATIC);

    type.getProperties().stream().filter(f -> !isASchemaUrlField(f)).forEach(f -> {
      if (!(isAProducerField(f))) {
        builderClassBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE);
        builderClassBuilder.addMethod(
            MethodSpec
                .methodBuilder(f.getName())
                .addParameter(getTypeName(f.getType()), f.getName())
                .addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription())
                .addModifiers(PUBLIC)
                .returns(ClassName.get(containerPackage, containerClassName, type.getName() + "Builder"))
                .addJavadoc("@return this\n")
                .addCode("this.$N = $N;\n", f.getName(), f.getName())
                .addCode("return this;")
                .build());
      }
    });
    if (type.hasAdditionalProperties()) {
      String fieldName = "additionalProperties";
      TypeName additionalPropertiesValueType = type.getAdditionalPropertiesType() == null ? ClassName.get(Object.class) : getTypeName(type.getAdditionalPropertiesType());
      TypeName additionalPropertiesType = ParameterizedTypeName.get(ClassName.get(Map.class), ClassName.get(String.class), additionalPropertiesValueType);


      builderClassBuilder.addField(
          FieldSpec.builder(additionalPropertiesType, fieldName, PRIVATE, FINAL)
              .initializer("new $T<>()", LinkedHashMap.class)
              .build());
      builderClassBuilder.addMethod(MethodSpec
          .methodBuilder("put")
          .addJavadoc("add additional properties\n")
          .addModifiers(PUBLIC)
          .returns(ClassName.get(containerPackage, containerClassName, type.getName() + "Builder"))
          .addParameter(TypeName.get(String.class), "key")
          .addJavadoc("@param key the additional property name\n")
          .addParameter(additionalPropertiesValueType, "value")
          .addJavadoc("@param value the additional property value\n")
          .addCode("this.$N.put(key, value);", fieldName)
          .addCode("return this;", fieldName)
          .addJavadoc("@return this\n")
          .build());
    }

    Builder build = builderBuildMethod(type);
    builderClassBuilder.addMethod(build.build());
    return builderClassBuilder.build();
  }

  private Builder builderBuildMethod(ObjectResolvedType type) {
    List<CodeBlock> builderParams = new ArrayList<>();
    type.getProperties().stream().filter(f -> !isASchemaUrlField(f)).forEach(f -> {
      if (isAProducerField(f)) {
        builderParams.add(CodeBlock.of(containerClassName + ".this.producer"));
      } else {
        builderParams.add(CodeBlock.of("$N", f.getName()));
      }
    });

    Builder build = MethodSpec
        .methodBuilder("build")
        .addModifiers(PUBLIC)
        .returns(getTypeName(type))
        .addCode("$N __result = new $N(", type.getName(), type.getName())
        .addCode(CodeBlock.join(builderParams, ", "))
        .addCode(");\n");

    if (type.hasAdditionalProperties()) {
      build.addCode("__result.getAdditionalProperties().putAll(additionalProperties);\n");
    }
    build.addCode("return __result;\n");
    return build;
  }

  private MethodSpec factoryModelMethodUnderContainer(ObjectResolvedType type) {
    Builder factory = MethodSpec.methodBuilder("new" + type.getName())
        .addModifiers(PUBLIC)
        .returns(getTypeName(type));

    List<CodeBlock> factoryParams = new ArrayList<>();

    type.getProperties().stream().filter(f -> !isASchemaUrlField(f)).forEach(f -> {
      if (isAProducerField(f)) {
        factoryParams.add(CodeBlock.of("this.producer"));
      } else {
        factory.addParameter(ParameterSpec.builder(getTypeName(f.getType()), f.getName()).build());
        factory.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
        factoryParams.add(CodeBlock.of("$N", f.getName()));
      }
    });
    factory.addJavadoc("@return $N", type.getName());
    factory.addCode("return new $N(", type.getName());
    factory.addCode(CodeBlock.join(factoryParams, ", "));
    factory.addCode(");\n");
    return factory.build();
  }

  private boolean isAProducerField(ResolvedField f) {
    return f.getName().equals("_producer") || f.getName().equals("producer");
  }

  private boolean isASchemaUrlField(ResolvedField f) {
    return !server && (f.getName().equals("_schemaURL") || f.getName().equals("schemaURL"));
  }

  private void generateInterface(TypeSpec.Builder containerTypeBuilder, ObjectResolvedType type) {
    TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(type.getName())
        .addModifiers(STATIC, PUBLIC);

    generateDefaultImplementation(containerTypeBuilder, type, interfaceBuilder);

    for (ResolvedField f : type.getProperties()) {
      MethodSpec getter = getter(f)
          .addModifiers(ABSTRACT, PUBLIC)
          .build();
      interfaceBuilder.addMethod(getter);
    }
    if (type.hasAdditionalProperties()) {
      String fieldName = "additionalProperties";
      TypeName additionalPropertiesValueType = getAdditionalPropertiesValueType(type);
      TypeName additionalPropertiesType = getAdditionalPropertiesType(additionalPropertiesValueType);
      interfaceBuilder.addMethod(MethodSpec
          .methodBuilder("get" + titleCase(fieldName))
          .addJavadoc("@return additional properties")
          .returns(additionalPropertiesType)
          .addModifiers(PUBLIC, ABSTRACT)
          .build());
    }
    TypeSpec intrfc = interfaceBuilder.build();

    containerTypeBuilder.addType(intrfc);
  }

  private void generateDefaultImplementation(
      TypeSpec.Builder containerTypeBuilder,
      ObjectResolvedType type,
      TypeSpec.Builder interfaceBuilder) {
    ///////////////////////////////////////////
    // Default implementation to deserialize //
    ///////////////////////////////////////////
    if (type.getName().endsWith("Facet") && !type.getName().equals("BaseFacet")) {
      // adding the annotation to the interface to have a default implementation
      interfaceBuilder.addAnnotation(AnnotationSpec.builder(JsonDeserialize.class)
          .addMember("as", CodeBlock.of("Default" + type.getName() + ".class"))
          .build());

      TypeSpec.Builder classBuilder = TypeSpec.classBuilder("Default" + type.getName())
          .addModifiers(STATIC, PUBLIC);
      classBuilder.addSuperinterface(ClassName.get(containerPackage, containerClassName, type.getName()));

      MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
          .addModifiers(PUBLIC);
      constructor.addAnnotation(JsonCreator.class);
      List<String> fieldNames = new ArrayList<String>();
      for (ResolvedField f : type.getProperties()) {
        classBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE, FINAL);
        fieldNames.add(f.getName());
        if (isASchemaUrlField(f)) {
          setSchemaURLField(type, constructor, f);
        } else {
          addConstructorParameter(constructor, f);
        }
        MethodSpec getter = getter(f)
            .addModifiers(PUBLIC)
            .addCode("return $N;", f.getName())
            .build();
        classBuilder.addMethod(getter);
      }

      // additionalFields
      if (type.hasAdditionalProperties()) {
        addAdditionalProperties(type, classBuilder, constructor);
      }
      classBuilder.addMethod(constructor.build());
      containerTypeBuilder.addType(classBuilder.build());


      Builder factory = MethodSpec.methodBuilder("new" + type.getName())
          .addModifiers(PUBLIC)
          .returns(getTypeName(type));

      List<CodeBlock> factoryParams = new ArrayList<>();

      type.getProperties().stream().filter(f -> !isASchemaUrlField(f)).forEach(f -> {
        if (isAProducerField(f)) {
          factoryParams.add(CodeBlock.of("this.producer"));
        } else {
          factory.addParameter(ParameterSpec.builder(getTypeName(f.getType()), f.getName()).build());
          factory.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
          factoryParams.add(CodeBlock.of("$N", f.getName()));
        }
      });
      factory.addJavadoc("@return $N", type.getName());
      factory.addCode("return new $N(", "Default" + type.getName());
      factory.addCode(CodeBlock.join(factoryParams, ", "));
      factory.addCode(");\n");
      containerTypeBuilder.addMethod(factory.build());
    }
    ///////////////////////////////
  }

  private void addParameterFromField(MethodSpec.Builder factory, ResolvedField f, AnnotationSpec annotationSpec) {
    com.squareup.javapoet.ParameterSpec.Builder paramSpecBuilder = ParameterSpec.builder(getTypeName(f.getType()), f.getName());
    if (annotationSpec != null) {
      paramSpecBuilder.addAnnotation(annotationSpec);
    }
    factory.addParameter(paramSpecBuilder.build());

    factory.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
  }

  private ParameterizedTypeName getAdditionalPropertiesType(
      TypeName additionalPropertiesValueType) {
    return ParameterizedTypeName.get(ClassName.get(Map.class), ClassName.get(String.class), additionalPropertiesValueType);
  }

  private TypeName getAdditionalPropertiesValueType(ObjectResolvedType type) {
    return type.getAdditionalPropertiesType() == null ? ClassName.get(Object.class) : getTypeName(type.getAdditionalPropertiesType());
  }

  private void addAdditionalProperties(
      ObjectResolvedType type, TypeSpec.Builder classBuilder, MethodSpec.Builder constructor) {
    String fieldName = "additionalProperties";
    TypeName additionalPropertiesValueType = getAdditionalPropertiesValueType(type);
    TypeName additionalPropertiesType = getAdditionalPropertiesType(additionalPropertiesValueType);
    classBuilder.addMethod(MethodSpec
        .methodBuilder("get" + titleCase(fieldName))
        .addJavadoc("@return additional properties")
        .returns(additionalPropertiesType)
        .addModifiers(PUBLIC)
        .addCode("return $N;", fieldName)
        .addAnnotation(AnnotationSpec.builder(JsonAnyGetter.class).build())
        .build());
    classBuilder.addField(
        FieldSpec.builder(additionalPropertiesType, fieldName, PRIVATE, FINAL)
        .addAnnotation(JsonAnySetter.class)
        .build());

    constructor.addCode(CodeBlock.builder().addStatement("this.$N = new $T<>()", fieldName, LinkedHashMap.class).build());
  }

  private void addConstructorParameter(MethodSpec.Builder constructor, ResolvedField f) {
    addParameterFromField(constructor, f, AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", f.getName()).build());
    constructor.addCode("this.$N = $N;\n", f.getName(), f.getName());
  }

  private void setSchemaURLField(
      ObjectResolvedType type, MethodSpec.Builder constructor, ResolvedField f) {
    String schemaURL = containerToID.get(type.getContainer()) + "#/$defs/" + type.getName();
    constructor.addCode("this.$N = URI.create($S);\n", f.getName(), schemaURL);
  }

  private Builder getter(ResolvedField f) {
    Builder builder = MethodSpec
        .methodBuilder("get" + titleCase(f.getName()))
        .returns(getTypeName(f.getType()));
    if (f.getDescription() != null) {
      builder.addJavadoc("@return $N", f.getDescription());
    }
    return builder;
  }


  public TypeName getTypeName(ResolvedType type) {
    return type.accept(new ResolvedTypeVisitor<TypeName>() {

      @Override
      public TypeName visit(PrimitiveResolvedType primitiveType) {
        if (primitiveType.getName().equals("integer")) {
          return ClassName.get(Long.class);
        } else if (primitiveType.getName().equals("number")) {
          return ClassName.get(Double.class);
        } else if (primitiveType.getName().equals("boolean")) {
          return ClassName.get(Boolean.class);
        } else if (primitiveType.getName().equals("string")) {
          if (primitiveType.getFormat() != null) {
            String format = primitiveType.getFormat();
            if (format.equals("uri")) {
              return ClassName.get(URI.class);
            } else if (format.equals("date-time")) {
              return ClassName.get(ZonedDateTime.class);
            } else if (format.equals("uuid")) {
              return ClassName.get(UUID.class);
            } else {
              throw new RuntimeException("Unknown format: " + primitiveType.getFormat());
            }
          }
          return ClassName.get(String.class);
        }
        throw new RuntimeException("Unknown primitive: " + primitiveType.getName());
      }

      @Override
      public TypeName visit(ObjectResolvedType objectType) {
        return ClassName.get(containerClass, objectType.getName());
      }

      @Override
      public TypeName visit(ArrayResolvedType arrayType) {
        return ParameterizedTypeName.get(ClassName.get(List.class), getTypeName(arrayType.getItems()));
      }
    });
  }
}
