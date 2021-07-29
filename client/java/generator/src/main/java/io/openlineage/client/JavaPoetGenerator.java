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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

  private static final String PACKAGE = "io.openlineage.client";

  private final TypeResolver typeResolver;
  private final URL baseURL;
  private final String containerClassName;
  private final String containerClass;

  public JavaPoetGenerator(TypeResolver typeResolver, String containerClassName, URL baseURL) {
    this.typeResolver = typeResolver;
    this.containerClassName = containerClassName;
    this.baseURL = baseURL;
    this.containerClass = PACKAGE + "." + containerClassName;
  }

  public void generate(PrintWriter printWriter) throws IOException {

    TypeSpec.Builder containerTypeBuilder = TypeSpec.classBuilder(containerClassName)
        .addModifiers(PUBLIC, FINAL);
    if (baseURL == null) {
      containerTypeBuilder.addJavadoc("$S", "Warning: this class was generated from a local file and will not provide absolute _schemaURL fields in facets");
    }

    containerTypeBuilder.addField(FieldSpec.builder(ClassName.get(URI.class), "producer", PRIVATE, FINAL).build());
    containerTypeBuilder.addMethod(MethodSpec.constructorBuilder()
      .addModifiers(PUBLIC)
      .addParameter(
        ParameterSpec.builder(ClassName.get(URI.class), "producer").build()
      )
      .addCode("this.producer = producer;\n")
      .build());

    generateTypes(containerTypeBuilder);
    TypeSpec openLineage = containerTypeBuilder.build();

    JavaFile javaFile = JavaFile.builder(PACKAGE, openLineage)
        .build();

    javaFile.writeTo(printWriter);
  }

  private void generateTypes(TypeSpec.Builder containerTypeBuilder) {
    Collection<ObjectResolvedType> types = typeResolver.getTypes();
    for (ObjectResolvedType type : types) {
      if (type.getName().length() == 0 || !type.getContainer().equals(containerClassName)) {
        // we're limiting ourselves to the types in the container we are generating
        continue;
      }
      if (typeResolver.getBaseTypes().contains(type.getName())) { // interfaces
        generateInterface(containerTypeBuilder, type);
      } else { // concrete types
        // We generate:
        // A data class
        // A factory method
        // A builder class


        TypeSpec.Builder builderClassBuilder = TypeSpec.classBuilder(type.getName() + "Builder")
            .addModifiers(PUBLIC, FINAL);
        List<CodeBlock> builderParams = new ArrayList<>();

        TypeSpec.Builder classBuilder = TypeSpec.classBuilder(type.getName())
            .addModifiers(STATIC, PUBLIC, FINAL);
        classBuilder.addAnnotation(AnnotationSpec.builder(JsonDeserialize.class)
            .addMember("as", CodeBlock.of(type.getName() + ".class"))
            .build());
        for (ObjectResolvedType parent : type.getParents()) {
          classBuilder.addSuperinterface(ClassName.get(PACKAGE, parent.getContainer(), parent.getName()));
        }
        MethodSpec.Builder factory = MethodSpec.methodBuilder("new" + type.getName())
            .addModifiers(PUBLIC)
            .returns(getTypeName(type));
        List<CodeBlock> factoryParams = new ArrayList<>();
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
            .addModifiers(PRIVATE);
        constructor.addAnnotation(JsonCreator.class);
        List<String> fieldNames = new ArrayList<String>();
        for (ResolvedField f : type.getProperties()) {
          classBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE, FINAL);
          fieldNames.add(f.getName());
          if (isSchemaURL(f)) {
            setSchemaURLField(type, constructor, f);
          } else {
            if (!isProducerField(f)) {
              builderClassBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE);
              Builder setterBuilder = MethodSpec.methodBuilder("set" + titleCase(f.getName())).addModifiers(PUBLIC);
              addParameterFromField(setterBuilder, f, null);
              setterBuilder.addCode("this.$N = $N;\n", f.getName(), f.getName());
              returnThis(setterBuilder, type.getName() + "Builder");
              builderClassBuilder.addMethod(setterBuilder.build());
            }
            addConstructorParameter(constructor, f);
            if (isProducerField(f)) {
              factoryParams.add(CodeBlock.of("this.producer"));
              builderParams.add(CodeBlock.of(containerClassName + ".this.producer"));
            } else {
              addParameterFromField(factory, f, null);
              factoryParams.add(CodeBlock.of("$N", f.getName()));
              builderParams.add(CodeBlock.of("$N", f.getName()));
            }
          }
          MethodSpec getter = getter(f)
              .addModifiers(PUBLIC)
              .addCode("return $N;", f.getName())
              .build();
          classBuilder.addMethod(getter);
        }
        factory.addJavadoc("@return $N", type.getName());
        factory.addCode("return new $N(", type.getName());
        factory.addCode(CodeBlock.join(factoryParams, ", "));
        factory.addCode(");\n");
        containerTypeBuilder.addMethod(factory.build());

        containerTypeBuilder.addMethod(MethodSpec.methodBuilder("new" + type.getName() + "Builder")
            .addModifiers(PUBLIC)
            .returns(ClassName.get(containerClass, type.getName() + "Builder"))
            .addCode("return new $N();", type.getName() + "Builder")
            .build());

        Builder build = MethodSpec
            .methodBuilder("build")
            .addModifiers(PUBLIC)
            .returns(getTypeName(type))
            .addCode("$N __result = new $N(", type.getName(), type.getName())
            .addCode(CodeBlock.join(builderParams, ", "))
            .addCode(");\n");

         // additionalFields
        if (type.hasAdditionalProperties()) {
          addAdditionalProperties(type, classBuilder, constructor);
          TypeName additionalPropertiesValueType = getAdditionalPropertiesValueType(type);
          TypeName additionalPropertiesType = getAdditionalPropertiesType(additionalPropertiesValueType);
          String fieldName = "additionalProperties";

          builderClassBuilder.addField(
              FieldSpec.builder(additionalPropertiesType, fieldName, PRIVATE, FINAL)
              .initializer("new $T<>()", HashMap.class)
              .build());
          Builder putBuilder = MethodSpec
              .methodBuilder("put")
              .addModifiers(PUBLIC)
              .addParameter(TypeName.get(String.class), "key")
              .addJavadoc("@param key the field name\n")
              .addParameter(additionalPropertiesValueType, "value")
              .addJavadoc("@param value the value\n")
              .addCode("this.$N.put(key, value);", fieldName);
          returnThis(putBuilder, type.getName() + "Builder");
          builderClassBuilder.addMethod(putBuilder.build());
          build.addCode("__result.getAdditionalProperties().putAll(additionalProperties);\n");
        }


        builderClassBuilder.addMethod(
            build
            .addCode("return __result;\n")
            .addJavadoc("@return a new $N", type.getName())
            .build());

        classBuilder.addMethod(constructor.build());
        containerTypeBuilder.addType(classBuilder.build());
        containerTypeBuilder.addType(builderClassBuilder.build());
      }
    }

  }

  private void generateInterface(TypeSpec.Builder containerTypeBuilder, ObjectResolvedType type) {
    TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(type.getName())
        .addModifiers(STATIC, PUBLIC);

    ///////////////////////////////////////////
    // Default implementation to deserialize //
    ///////////////////////////////////////////
    if (type.getName().endsWith("Facet") && !type.getName().equals("BaseFacet")) {
      // adding the annotation to the interface to have a default implementation
      interfaceBuilder.addAnnotation(AnnotationSpec.builder(JsonDeserialize.class)
          .addMember("as", CodeBlock.of("Default" + type.getName() + ".class"))
          .build());

      TypeSpec.Builder classBuilder = TypeSpec.classBuilder("Default" + type.getName())
          .addModifiers(STATIC, PRIVATE, FINAL);
      classBuilder.addSuperinterface(ClassName.get(PACKAGE, containerClassName, type.getName()));

      MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
          .addModifiers(PRIVATE);
      constructor.addAnnotation(JsonCreator.class);
      List<String> fieldNames = new ArrayList<String>();
      for (ResolvedField f : type.getProperties()) {
        classBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE, FINAL);
        fieldNames.add(f.getName());
        if (isSchemaURL(f)) {
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
    }
    ///////////////////////////////

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

  private void returnThis(Builder methodBuilder, String typeName) {
    methodBuilder.returns(ClassName.get(containerClass, typeName))
      .addJavadoc("@return this\n")
      .addCode("return this;");
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

  private boolean isProducerField(ResolvedField f) {
    return f.getName().equals("_producer") || f.getName().equals("producer");
  }

  private void addConstructorParameter(MethodSpec.Builder constructor, ResolvedField f) {
    addParameterFromField(constructor, f, AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", f.getName()).build());
    constructor.addCode("this.$N = $N;\n", f.getName(), f.getName());
  }

  private boolean isSchemaURL(ResolvedField f) {
    return f.getName().equals("_schemaURL");
  }

  private void setSchemaURLField(
      ObjectResolvedType type, MethodSpec.Builder constructor, ResolvedField f) {
    String schemaURL = baseURL + "#/$defs/" + type.getName();
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
    return type.accept(new ResolvedTypeVisitor<TypeName>(){

      @Override
      public TypeName visit(PrimitiveResolvedType primitiveType) {
        if (primitiveType.getName().equals("string")) {
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
