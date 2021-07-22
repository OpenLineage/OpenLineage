package openlineage.spark.agent.transformers;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import lombok.extern.slf4j.Slf4j;
import openlineage.spark.agent.OpenLineageSparkListener;

@Slf4j
public class SparkContextTransformer implements ClassFileTransformer {
  private final String className = "org.apache.spark.SparkContext";
  private final String internalForm = className.replaceAll("\\.", "/");

  public static final String CODE =
      String.format("{ %s.instrument(this); }", OpenLineageSparkListener.class.getName());

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer)
      throws IllegalClassFormatException {
    if (!className.equals(this.internalForm)) {
      return classfileBuffer;
    }
    log.info("SparkContextTransformer.transform(" + className + ")");
    try {
      CtClass ctClass =
          ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer), true);

      CtConstructor[] constructors = ctClass.getConstructors();
      for (CtConstructor constructor : constructors) {
        if (constructor.callsSuper()) {
          constructor.insertAfter(CODE);
        }
      }
      return ctClass.toBytecode();
    } catch (Throwable throwable) {
      log.error("Failed to instrument " + className + ". Not doing anything", throwable);
      return classfileBuffer;
    }
  }
}
