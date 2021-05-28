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
  private static final String CONTAINER_CLASS_NAME = "OpenLineage";
  private static final String CONTAINER_CLASS = PACKAGE + "." + CONTAINER_CLASS_NAME;
  private final TypeResolver typeResolver;
  private final String baseURL;

  public JavaPoetGenerator(TypeResolver typeResolver, String baseURL) {
    this.typeResolver = typeResolver;
    this.baseURL = baseURL;
  }

  public void generate(PrintWriter printWriter) throws IOException {

    TypeSpec.Builder containerTypeBuilder = TypeSpec.classBuilder(CONTAINER_CLASS_NAME)
        .addModifiers(PUBLIC, FINAL);
    if (baseURL.equals("")) {
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

      if (typeResolver.getBaseTypes().contains(type.getName())) {
        TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(type.getName())
            .addModifiers(STATIC, PUBLIC);
        for (ResolvedField f : type.getProperties()) {
          MethodSpec getter = getter(f)
              .addModifiers(ABSTRACT, PUBLIC)
              .build();
          interfaceBuilder.addMethod(getter);
        }
        TypeSpec intrfc = interfaceBuilder.build();
        containerTypeBuilder.addType(intrfc);
      } else {
        TypeSpec.Builder builderClassBuilder = TypeSpec.classBuilder(type.getName() + "Builder")
            .addModifiers(PUBLIC, FINAL);
        List<CodeBlock> builderParams = new ArrayList<>();

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
        for (ResolvedField f : type.getProperties()) {
          classBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE, FINAL);
          fieldNames.add(f.getName());
          if (f.getName().equals("_schemaURL")) {
            String schemaURL = baseURL + "#/definitions/" + type.getName();
            constructor.addCode("this.$N = URI.create($S);\n", f.getName(), schemaURL);
          } else {
            if (!(f.getName().equals("_producer") || f.getName().equals("producer"))) {
              builderClassBuilder.addField(getTypeName(f.getType()), f.getName(), PRIVATE);
              builderClassBuilder.addMethod(
                  MethodSpec
                  .methodBuilder("set" + titleCase(f.getName()))
                  .addParameter(getTypeName(f.getType()), f.getName())
                  .addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription())
                  .addModifiers(PUBLIC)
                  .returns(ClassName.get(CONTAINER_CLASS, type.getName() + "Builder"))
                  .addJavadoc("@return this\n")
                  .addCode("this.$N = $N;\n", f.getName(), f.getName())
                  .addCode("return this;")
                  .build());
            }
            constructor.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
            constructor.addParameter(
                ParameterSpec.builder(getTypeName(f.getType()), f.getName())
                .addAnnotation(AnnotationSpec.builder(JsonProperty.class).addMember("value", "$S", f.getName()).build())
                .build());
            constructor.addCode("this.$N = $N;\n", f.getName(), f.getName());
            if (f.getName().equals("_producer") || f.getName().equals("producer")) {
              factoryParams.add(CodeBlock.of("this.producer"));
              builderParams.add(CodeBlock.of("OpenLineage.this.producer"));
            } else {
              factory.addParameter(ParameterSpec.builder(getTypeName(f.getType()), f.getName()).build());
              factory.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "the " + f.getName() : f.getDescription());
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
            .returns(ClassName.get(CONTAINER_CLASS, type.getName() + "Builder"))
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

          builderClassBuilder.addField(
              FieldSpec.builder(additionalPropertiesType, fieldName, PRIVATE, FINAL)
              .initializer("new $T<>()", HashMap.class)
              .build());
          builderClassBuilder.addMethod(MethodSpec
              .methodBuilder("put")
              .addModifiers(PUBLIC)
              .addParameter(TypeName.get(String.class), "key")
              .addParameter(additionalPropertiesValueType, "value")
              .addCode("this.$N.put(key, value);", fieldName)
              .build());

          build.addCode("__result.getAdditionalProperties().putAll(additionalProperties);\n");

          constructor.addCode(CodeBlock.builder().addStatement("this.$N = new $T<>()", fieldName, HashMap.class).build());
        }


        builderClassBuilder.addMethod(
            build
            .addCode("return __result;\n")
            .build());

        classBuilder.addMethod(constructor.build());
        containerTypeBuilder.addType(classBuilder.build());
        containerTypeBuilder.addType(builderClassBuilder.build());
      }
    }

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

  public static TypeName getTypeName(ResolvedType type) {
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
        return ClassName.get(CONTAINER_CLASS, objectType.getName());
      }

      @Override
      public TypeName visit(ArrayResolvedType arrayType) {
        return ParameterizedTypeName.get(ClassName.get(List.class), getTypeName(arrayType.getItems()));
      }
    });
  }
}
