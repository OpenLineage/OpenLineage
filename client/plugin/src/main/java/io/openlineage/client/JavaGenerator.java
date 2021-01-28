package io.openlineage.client;

import static io.openlineage.client.TypeResolver.titleCase;

import java.util.Collection;

import io.openlineage.client.TypeResolver.ArrayType;
import io.openlineage.client.TypeResolver.Field;
import io.openlineage.client.TypeResolver.ObjectType;
import io.openlineage.client.TypeResolver.PrimitiveType;
import io.openlineage.client.TypeResolver.Type;
import io.openlineage.client.TypeResolver.TypeVisitor;

public class JavaGenerator {

  private int indent = 0;
  private TypeResolver typeResolver;

  public JavaGenerator(TypeResolver typeResolver) {
    this.typeResolver = typeResolver;
  }


  public void generate() {

//    comment("Generating:");
    gen("package io.openlineage.client;");
    gen("");
    gen("import java.util.List;");
    gen("");
    gen("public final class OpenLineage {");
    indent ++;
    Collection<ObjectType> types = typeResolver.getTypes();
    for (ObjectType type : types) {
//      comment("generate: " + type);
      gen("");
      generate(type);
    }
    indent --;
    gen("}");
  }

  private void generate(ObjectType type) {
    if (typeResolver.getBaseTypes().contains(type.getName())) {
      gen("static interface %s {", type.getName());
      indent ++;
      for (Field f : type.getProperties()) {
        if (f.getDescription()!=null) {
          gen("/** %s */", f.getDescription());
        }
        gen("%s get%s();", getTypeName(f.getType()), titleCase(f.getName()));
      }
    } else {
      String base = type.getParents().size() !=0 ? " implements " + String.join(", ", type.getParents()) : "";
      gen("static final class %s%s {", type.getName(), base);
      indent ++;
      gen("");
      // fields
      for (Field f : type.getProperties()) {
        gen("private final %s %s;", getTypeName(f.getType()), f.getName());
      }
      gen("");
      // constructor
      gen("/**");
      for (Field f : type.getProperties()) {
        gen(" * @param %s %s", f.getName(), f.getDescription() == null ? "" : f.getDescription());
      }
      gen(" */");
      gen("public %s(", type.getName());
      int l = type.getProperties().size();
      int i = 0;
      for (Field f : type.getProperties()) {
        ++i;
        gen("  %s %s%s", getTypeName(f.getType()), f.getName(), i==l ? "" : ",");
      }
      gen(") {");
      for (Field f : type.getProperties()) {
        gen("  this.%s=%s;", f.getName(), f.getName());
      }
      gen("}");
      gen("");
      // getters
      for (Field f : type.getProperties()) {
        if (f.getDescription()!=null) {
          gen("/** %s */", f.getDescription());
        }
        gen("public %s get%s() { return %s; }", getTypeName(f.getType()), titleCase(f.getName()), f.getName());
      }
      gen("");
    }
    indent --;
    gen("}");
  }

  public static String getTypeName(Type type) {
    return type.accept(new TypeVisitor<String>(){

      @Override
      public String visit(PrimitiveType primitiveType) {
        return primitiveType.getName();
      }

      @Override
      public String visit(ObjectType objectType) {
        return objectType.getName();
      }

      @Override
      public String visit(ArrayType arrayType) {
        return "List<" + getTypeName(arrayType.getItems()) + ">";
      }
    });
  }


  private void gen(String pattern, Object... params) {
    for (int i = 0; i < indent; i++) {
      System.out.print("  ");
    }
    System.out.printf(pattern + "\n", params);

  }

  private void comment(Object pattern) {
    gen("// " + pattern);
  }
}
