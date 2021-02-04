package marquez.spark.agent.transformers;

import java.io.ByteArrayInputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import lombok.extern.slf4j.Slf4j;
import marquez.spark.agent.SparkListener;

@Slf4j
public class ActiveJobTransformer implements ClassFileTransformer {
  private static final String className = "org.apache.spark.scheduler.ActiveJob";
  private final String internalForm = className.replaceAll("\\.", "/");

  public static final String CODE =
      String.format("{ %s.registerActiveJob(this); }", SparkListener.class.getName());

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
    log.info("ActiveJobTransformer.transform(" + className + ")");
    try {
      CtClass ctClass = ClassPool.getDefault().makeClass(new ByteArrayInputStream(classfileBuffer));

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
