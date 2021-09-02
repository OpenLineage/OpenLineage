package io.openlineage.spark.agent.transformers;

import io.openlineage.spark.agent.lifecycle.plan.DatasetSource;
import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.Arrays;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ClassFileTransformer} that rewrites both the {@link
 * com.google.cloud.spark.bigquery.BigQueryRelation} class as well as the v2 {@link
 * com.google.cloud.spark.bigquery.v2.BigQueryDataSourceReader} class to implement the {@link
 * DatasetSource} interface by introducing the {@link DatasetSource#name()} method, returning the
 * fully qualified table name (&lt;projectId&gt;.&lt;dataset&gt;.&lt;table&gt;), and the {@link
 * DatasetSource#namespace()} method, returning the literal string "bigquery".
 */
@Slf4j
public class BigQueryRelationTransformer implements ClassFileTransformer {

  /** Namespace method- common to all BQ sources. */
  private static final String BQ_NAMESPACE_METHOD =
      "public String namespace() { return \"bigquery\"; }";

  /**
   * The name() method implementation for {@link com.google.cloud.spark.bigquery.BigQueryRelation}
   */
  private static final String BQ_RELATION_NAME_METHOD =
      "public String name() { return tableName(); }";

  /**
   * The name() method implementation for {@link
   * com.google.cloud.spark.bigquery.v2.BigQueryDataSourceReader}
   */
  private static final String BQ_DS_READER_NAME_METHOD =
      "public String name() { "
          + "return String.format(\"%s.%s.%s\", "
          + "tableId.getProject(), tableId.getDataset(), tableId.getTable());}";

  /**
   * The name() method implementation for {@link
   * com.google.cloud.spark.bigquery.v2.BigQueryIndirectDataSourceWriter}
   */
  private static final String BQ_DS_WRITER_NAME_METHOD =
      "public String name() { "
          + "return String.format(\"%s.%s.%s\", "
          + "config.getTableId().getProject(), config.getTableId().getDataset(), config.getTableId().getTable());}";

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protOpenLineageDaoectionDomain,
      byte[] classfileBuffer)
      throws IllegalClassFormatException {

    switch (className) {
      case "com/google/cloud/spark/bigquery/BigQueryRelation":
        log.info("Rewriting BigQueryRelation");
        return rewrite(classfileBuffer, BQ_RELATION_NAME_METHOD, BQ_NAMESPACE_METHOD);
      case "com/google/cloud/spark/bigquery/BigQueryDataSourceReader":
        log.info("Rewriting BigQueryDataSourceReader");
        return rewrite(classfileBuffer, BQ_DS_READER_NAME_METHOD, BQ_NAMESPACE_METHOD);
      case "com/google/cloud/spark/bigquery/v2/BigQueryIndirectDataSourceWriter":
        log.info("Rewriting BigQueryIndirectDataSourceWriter");
        return rewrite(classfileBuffer, BQ_DS_WRITER_NAME_METHOD, BQ_NAMESPACE_METHOD);
    }
    return classfileBuffer;
  }

  private byte[] rewrite(byte[] classfileBuffer, String nameMethod, String namespaceMethod) {
    try {
      CtClass ctClass =
          ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer), true);
      CtClass[] interfaces = ctClass.getInterfaces();
      if (interfaces == null) {
        interfaces = new CtClass[1];
      } else {
        interfaces = Arrays.copyOf(interfaces, interfaces.length + 1);
      }
      interfaces[interfaces.length - 1] = ClassPool.getDefault().get(DatasetSource.class.getName());

      ctClass.setInterfaces(interfaces);
      ctClass.addMethod(CtMethod.make(nameMethod, ctClass));
      ctClass.addMethod(CtMethod.make(namespaceMethod, ctClass));
      return ctClass.toBytecode();
    } catch (Throwable t) {
      log.error("Unable to rewrite class", t);
    }
    return classfileBuffer;
  }
}
