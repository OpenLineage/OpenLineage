package openlineage.generator;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.openapitools.codegen.CodegenConfig;
import org.openapitools.codegen.CodegenConstants;
import org.openapitools.codegen.CodegenOperation;
import org.openapitools.codegen.CodegenType;
import org.openapitools.codegen.DefaultCodegen;

public class OpenLineageGenerator extends DefaultCodegen implements CodegenConfig {

  // source folder where to write the files
  protected String sourceFolder = "src/main/java";
  protected String apiVersion = "0.1.0";

  /**
   * Configures the type of generator.
   *
   * @return  the CodegenType for this generator
   * @see     org.openapitools.codegen.CodegenType
   */
  public CodegenType getTag() {
    return CodegenType.CLIENT;
  }

  /**
   * Configures a friendly name for the generator.  This will be used by the generator
   * to select the library with the -g flag.
   *
   * @return the friendly name for the generator
   */
  public String getName() {
    return "open-lineage";
  }

  /**
   * Provides an opportunity to inspect and modify operation data before the code is generated.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> postProcessOperationsWithModels(Map<String, Object> objs, List<Object> allModels) {

    // to try debugging your code generator:
    // set a break point on the next line.
    // then debug the JUnit test called LaunchGeneratorInDebugger

    Map<String, Object> results = super.postProcessOperationsWithModels(objs, allModels);

    Map<String, Object> ops = (Map<String, Object>)results.get("operations");
    ArrayList<CodegenOperation> opList = (ArrayList<CodegenOperation>)ops.get("operation");

    // iterate over the operation and perhaps modify something
    for(CodegenOperation co : opList){
      // example:
      // co.httpMethod = co.httpMethod.toLowerCase();
    }

    return results;
  }

  /**
   * Returns human-friendly help for the generator.  Provide the consumer with help
   * tips, parameters here
   *
   * @return A string value for the help message
   */
  public String getHelp() {
    return "Generates a open-lineage client library.";
  }

  public OpenLineageGenerator() {
    super();
    setReservedWordsLowerCase(
        Arrays.asList(
            // special words
            "object",

            // language reserved words
            "abstract", "continue", "for", "new", "switch", "assert",
            "default", "if", "package", "synchronized", "boolean", "do", "goto", "private",
            "this", "break", "double", "implements", "protected", "throw", "byte", "else",
            "import", "public", "throws", "case", "enum", "instanceof", "return", "transient",
            "catch", "extends", "int", "short", "try", "char", "final", "interface", "static",
            "void", "class", "finally", "long", "strictfp", "volatile", "const", "float",
            "native", "super", "while", "null")
        );

    languageSpecificPrimitives = new HashSet<String>(
        Arrays.asList(
            "enum",
            "String",
            "boolean",
            "Boolean",
            "Double",
            "Integer",
            "Long",
            "Float",
            "Object",
            "byte[]")
        );
//    instantiationTypes.put("array", "ArrayList");
    instantiationTypes.put("set", "LinkedHashSet");
    instantiationTypes.put("map", "HashMap");

    typeMapping.put("date", "Date");
    typeMapping.put("file", "File");
    typeMapping.put("AnyType", "Object");
    typeMapping.put("enum", "String"); ///????
    typeMapping.put("array", "List");
    typeMapping.put("set", "Set");
    typeMapping.put("map", "Map");
    typeMapping.put("DateTime", "java.util.Date");
    typeMapping.put("UUID", "java.util.UUID");
    typeMapping.put("string", "String");

    importMapping.put("enum", "java.lang.String"); /// ???
    importMapping.put("string", "java.lang.String"); /// ???
    importMapping.put("object", "java.lang.Object"); /// ???
    importMapping.put("array", "java.util.List"); /// ???
    importMapping.put("BigDecimal", "java.math.BigDecimal");
    importMapping.put("URI", "java.net.URI");
    importMapping.put("File", "java.io.File");
    importMapping.put("Date", "java.util.Date");
    importMapping.put("Timestamp", "java.sql.Timestamp");
    importMapping.put("Map", "java.util.Map");
    importMapping.put("HashMap", "java.util.HashMap");
    importMapping.put("Array", "java.util.List");
    importMapping.put("ArrayList", "java.util.ArrayList");
    importMapping.put("List", "java.util.*");
    importMapping.put("Set", "java.util.*");
    importMapping.put("LinkedHashSet", "java.util.LinkedHashSet");

    importMapping.put("ToStringSerializer", "com.fasterxml.jackson.databind.ser.std.ToStringSerializer");
    importMapping.put("JsonSerialize", "com.fasterxml.jackson.databind.annotation.JsonSerialize");

    // imports for pojos
    importMapping.put("ApiModelProperty", "io.swagger.annotations.ApiModelProperty");
    importMapping.put("ApiModel", "io.swagger.annotations.ApiModel");
    importMapping.put("BigDecimal", "java.math.BigDecimal");
    importMapping.put("JsonProperty", "com.fasterxml.jackson.annotation.JsonProperty");
    importMapping.put("JsonSubTypes", "com.fasterxml.jackson.annotation.JsonSubTypes");
    importMapping.put("JsonTypeInfo", "com.fasterxml.jackson.annotation.JsonTypeInfo");
    importMapping.put("JsonTypeName", "com.fasterxml.jackson.annotation.JsonTypeName");
    importMapping.put("JsonCreator", "com.fasterxml.jackson.annotation.JsonCreator");
    importMapping.put("JsonValue", "com.fasterxml.jackson.annotation.JsonValue");
    importMapping.put("JsonIgnore", "com.fasterxml.jackson.annotation.JsonIgnore");
    importMapping.put("JsonInclude", "com.fasterxml.jackson.annotation.JsonInclude");
    importMapping.put("IOException", "java.io.IOException");
    importMapping.put("Objects", "java.util.Objects");
    importMapping.put("com.fasterxml.jackson.annotation.JsonProperty", "com.fasterxml.jackson.annotation.JsonCreator");
    // set the output folder here
    outputFolder = "generated-code/open-lineage";

    additionalProperties.put(CodegenConstants.ARTIFACT_ID, "openlineage-api");

    /**
     * Models.  You can write model files using the modelTemplateFiles map.
     * if you want to create one template for file, you can do so here.
     * for multiple files for model, just put another entry in the `modelTemplateFiles` with
     * a different extension
     */
    additionalProperties.put(CodegenConstants.SOURCE_FOLDER, "src/main/java");
    modelTemplateFiles.put(
      "model.mustache", // the template to use
      ".java");       // the extension for each file to write

    /**
     * Api classes.  You can write classes for each Api file with the apiTemplateFiles map.
     * as with models, add multiple entries with different extensions for multiple files per
     * class
     */
    apiTemplateFiles.put(
      "api.mustache",   // the template to use
      ".java");       // the extension for each file to write

    /**
     * Template Location.  This is the location which templates will be read from.  The generator
     * will use the resource stream to attempt to read the templates.
     */
    templateDir = "open-lineage";

    /**
     * Api Package.  Optional, if needed, this can be used in templates
     */
    apiPackage = "openlineage.api";

    /**
     * Model Package.  Optional, if needed, this can be used in templates
     */
    modelPackage = "openlineage.model";

    /**
     * Additional Properties.  These values can be passed to the templates and
     * are available in models, apis, and supporting files
     */
    additionalProperties.put("apiVersion", apiVersion);


  }

  /**
   * Escapes a reserved word as defined in the `reservedWords` array. Handle escaping
   * those terms here.  This logic is only called if a variable matches the reserved words
   *
   * @return the escaped term
   */
  @Override
  public String escapeReservedWord(String name) {
    return "_" + name;  // add an underscore to the name
  }

  /**
   * Location to write model files.  You can use the modelPackage() as defined when the class is
   * instantiated
   */
  public String modelFileFolder() {
    return outputFolder + "/" + sourceFolder + "/" + modelPackage().replace('.', File.separatorChar);
  }

  /**
   * Location to write api files.  You can use the apiPackage() as defined when the class is
   * instantiated
   */
  @Override
  public String apiFileFolder() {
    return outputFolder + "/" + sourceFolder + "/" + apiPackage().replace('.', File.separatorChar);
  }

  /**
   * override with any special text escaping logic to handle unsafe
   * characters so as to avoid code injection
   *
   * @param input String to be cleaned up
   * @return string with unsafe characters removed or escaped
   */
  @Override
  public String escapeUnsafeCharacters(String input) {
    //TODO: check that this logic is safe to escape unsafe characters to avoid code injection
    return input;
  }

  /**
   * Escape single and/or double quote to avoid code injection
   *
   * @param input String to be cleaned up
   * @return string with quotation mark removed or escaped
   */
  public String escapeQuotationMark(String input) {
    //TODO: check that this logic is safe to escape quotation mark to avoid code injection
    return input.replace("\"", "\\\"");
  }
}