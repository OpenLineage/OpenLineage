package io.openlineage.client;

import static io.openlineage.client.TypeResolver.titleCase;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;

import java.io.IOException;
import java.util.Collection;

import javax.lang.model.element.Modifier;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.TypeSpec;

import io.openlineage.client.TypeResolver.Field;
import io.openlineage.client.TypeResolver.ObjectType;
import io.openlineage.client.TypeResolver.Type;

public class JavaPoetGenerator {

  private TypeResolver typeResolver;

  public JavaPoetGenerator(TypeResolver typeResolver) {
    this.typeResolver = typeResolver;
  }

  public void generate() throws IOException {

    TypeSpec.Builder builder = TypeSpec.classBuilder("OpenLineage")
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL);
    generateTypes(builder);
    TypeSpec openLineage = builder.build();

    JavaFile javaFile = JavaFile.builder("io.openlineage.client", openLineage)
        .build();

    javaFile.writeTo(System.out);
  }

  private void generateTypes(TypeSpec.Builder builder) {
    Collection<ObjectType> types = typeResolver.getTypes();
    for (ObjectType type : types) {

      if (typeResolver.getBaseTypes().contains(type.getName())) {
        TypeSpec.Builder interfaceBuilder = TypeSpec.interfaceBuilder(type.getName())
            .addModifiers(Modifier.STATIC);
        for (Field f : type.getProperties()) {
          MethodSpec getter = getter(f)
              .addModifiers(Modifier.ABSTRACT, Modifier.PUBLIC)
              .build();
          interfaceBuilder.addMethod(getter);
        }
        TypeSpec intrfc = interfaceBuilder.build();
        builder.addType(intrfc);
      } else {
        TypeSpec.Builder classBuilder = TypeSpec.classBuilder(type.getName())
            .addModifiers(Modifier.STATIC, Modifier.FINAL);
        for (String parent : type.getParents()) {
          classBuilder.addSuperinterface(ClassName.get("io.openlineage.client", parent));
        }
        MethodSpec.Builder constructor = MethodSpec.constructorBuilder();
        for (Field f : type.getProperties()) {
          MethodSpec getter = getter(f)
              .addModifiers(Modifier.PUBLIC)
              .addCode("return $N;", f.getName())
              .build();
          classBuilder.addMethod(getter);
          classBuilder.addField(className(f.getType()), f.getName(), PRIVATE, FINAL);
          constructor.addParameter(className(f.getType()), f.getName());
          constructor.addCode("this.$N = $N;\n", f.getName(), f.getName());
          constructor.addJavadoc("@param $N $N\n", f.getName(), f.getDescription() == null ? "" : f.getDescription());
        }
        classBuilder.addMethod(constructor.build());
        TypeSpec clss = classBuilder.build();
        builder.addType(clss);
      }
    }

  }

  ClassName className(Type type) {
    return ClassName.get("io.openlineage.client", JavaGenerator.getTypeName(type));
  }

  private Builder getter(Field f) {
    Builder builder = MethodSpec
        .methodBuilder("get" + titleCase(f.getName()))
        .returns(className(f.getType()));
    if (f.getDescription() != null) {
      builder.addJavadoc("$N", f.getDescription());
    }
    return builder;


  }
}
