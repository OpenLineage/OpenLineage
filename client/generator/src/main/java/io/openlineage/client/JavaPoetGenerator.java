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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

import io.openlineage.client.TypeResolver.ArrayType;
import io.openlineage.client.TypeResolver.Field;
import io.openlineage.client.TypeResolver.ObjectType;
import io.openlineage.client.TypeResolver.PrimitiveType;
import io.openlineage.client.TypeResolver.Type;
import io.openlineage.client.TypeResolver.TypeVisitor;

/**
 * Generates a JavaClass with all the types as inner classes
 */
public class JavaPoetGenerator {

  private static final String PACKAGE = "io.openlineage.client";
  private static final String CONTAINER_CLASS_NAME = "OpenLineage";
  private static final String CONTAINER_CLASS = PACKAGE + "." + CONTAINER_CLASS_NAME;
  private final TypeResolver typeResolver;
  private final String baseURL;

  public JavaPoetGenerator(TypeResolver typeResolver, String baseURL) {
    this.typeResolver = typeResolver;
    this.baseURL = baseURL;
  }

  public void generate(PrintWriter printWriter) throws IOException {

    TypeSpec.Builder builder = TypeSpec.classBuilder(CONTAINER_CLASS_NAME)
        .addModifiers(PUBLIC, FINAL);
    if (baseURL.equals("")) {
      builder.addJavadoc("$S", "Warning: this class was generated from a local file and will not provide absolute _schemaURL fields in facets");

    }

    builder.addField(FieldSpec.builder(ClassName.get(URI.class), "producer", PRIVATE, FINAL).build());
    builder.addMethod(MethodSpec.constructorBuilder()
      .addModifiers(PUBLIC)
      .addParameter(
        ParameterSpec.builder(ClassName.get(URI.class), "producer").build()
      )
      .addCode("this.producer = producer;\n")
      .build());

    generateTypes(builder);
    TypeSpec openLineage = builder.build();

    JavaFile javaFile = JavaFile.builder(PACKAGE, openLineage)
        .build();

    javaFile.writeTo(printWriter);
  }

  private void generateTypes(TypeSpec.Builder builder) {
    Collection<ObjectType> types = typeResolver.getTypes();
    for (ObjectType type : types) {

      if (typeResolver.getBaseTypes().contains(type.getName())) {
        TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(type.getName())
            .addModifiers(STATIC, PUBLIC);
        for (Field f : type.getProperties()) {
          MethodSpec getter = getter(f)
              .addModifiers(ABSTRACT, PUBLIC)
              .build();
          interfaceBuilder.addMethod(getter);
        }
        TypeSpec intrfc = interfaceBuilder.build();
        builder.addType(intrfc);
      } else {
        TypeSpec.Builder classBuilder = TypeSpec.classBuilder(type.getName())
            .addModifiers(STATIC, PUBLIC, FINAL);
        for (String parent : type.getParents()) {
          classBuilder.addSuperinterface(ClassName.get(CONTAINER_CLASS, parent));
        }
        MethodSpec.Builder factory = MethodSpec.methodBuilder("new" + type.getName())
            .addModifiers(PUBLIC)
            .returns(getTypeName(type));
        List<CodeBlock> factoryParams = new ArrayList<>();
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
            .addModifiers(PRIVATE);
        constructor.addAnnotation(JsonCreator.class);
        List<String> fieldNames = new ArrayList<String>();
        for (Field f : type.getProperties()) {
          classBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE, FINAL);
          fieldNames.add(f.getName());
          if (f.getName().equals("_schemaURL")) {
            String schemaURL = baseURL + "#/definitions/" + type.getName();
            constructor.addCode("this.$N = URI.create($S);\n", f.getName(), schemaURL);
          } else {
            constructor.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
            constructor.addParameter(
                ParameterSpec.builder(getTypeName(f.getType()), f.getName())
                .addAnnotation(AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", f.getName()).build())
                .build());
            constructor.addCode("this.$N = $N;\n", f.getName(), f.getName());
            if (f.getName().equals("_producer") || f.getName().equals("producer")) {
              factoryParams.add(CodeBlock.of("this.producer"));
            } else {
              factory.addParameter(ParameterSpec.builder(getTypeName(f.getType()), f.getName()).build());
              factory.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
              factoryParams.add(CodeBlock.of("$N", f.getName()));
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
        builder.addMethod(factory.build());

         // additionalFields
        if (type.isHasAdditionalProperties()) {
          String fieldName = "additionalProperties";
          TypeName additionalPropertiesValueType = type.getAdditionalPropertiesType() == null ? ClassName.get(Object.class) : getTypeName(type.getAdditionalPropertiesType());
          TypeName additionalPropertiesType = ParameterizedTypeName.get(ClassName.get(Map.class), ClassName.get(String.class), additionalPropertiesValueType);
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

          constructor.addCode(CodeBlock.builder().addStatement("this.$N = new $T<>()", fieldName, HashMap.class).build());
        }

        classBuilder.addMethod(constructor.build());
        TypeSpec clss = classBuilder.build();
        builder.addType(clss);
      }
    }

  }

  private Builder getter(Field f) {
    Builder builder = MethodSpec
        .methodBuilder("get" + titleCase(f.getName()))
        .returns(getTypeName(f.getType()));
    if (f.getDescription() != null) {
      builder.addJavadoc("@return $N", f.getDescription());
    }
    return builder;
  }

  public static TypeName getTypeName(Type type) {
    return type.accept(new TypeVisitor<TypeName>(){

      @Override
      public TypeName visit(PrimitiveType primitiveType) {
        if (primitiveType.getName().equals("string")) {
          if (primitiveType.getFormat() != null) {
            String format = primitiveType.getFormat();
            if (format.equals("uri")) {
              return ClassName.get(URI.class);
            } else if (format.equals("date-time")) {
              return ClassName.get(ZonedDateTime.class);
            } else {
              throw new RuntimeException("Unknown format: " + primitiveType.getFormat());
            }
          }
          return ClassName.get(String.class);
        }
        throw new RuntimeException("Unknown primitive: " + primitiveType.getName());
      }

      @Override
      public TypeName visit(ObjectType objectType) {
        return ClassName.get(CONTAINER_CLASS, objectType.getName());
      }

      @Override
      public TypeName visit(ArrayType arrayType) {
        return ParameterizedTypeName.get(ClassName.get(List.class), getTypeName(arrayType.getItems()));
      }
    });
  }
}
