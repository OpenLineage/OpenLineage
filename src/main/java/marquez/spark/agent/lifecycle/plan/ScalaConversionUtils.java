package marquez.spark.agent.lifecycle.plan;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

/** Simple conversion utilities for dealing with Scala types */
public class ScalaConversionUtils {

  /**
   * Apply a map function to a {@link Seq}. This consolidates the silliness in converting between
   * Scala and Java collections.
   *
   * @param seq
   * @param fn
   * @param <R>
   * @param <T>
   * @return
   */
  public static <R, T> Seq<R> map(Seq<T> seq, Function<T, R> fn) {
    return fromList(fromSeq(seq).stream().map(fn).collect(Collectors.toList()));
  }

  /**
   * Convert a {@link List} to a Scala {@link Seq}.
   *
   * @param list
   * @param <T>
   * @return
   */
  public static <T> Seq<T> fromList(List<T> list) {
    return JavaConverters.asScalaBufferConverter(list).asScala();
  }

  /**
   * Convert a {@link Seq} to a Java {@link List}.
   *
   * @param seq
   * @param <T>
   * @return
   */
  public static <T> List<T> fromSeq(Seq<T> seq) {
    return JavaConverters.bufferAsJavaListConverter(seq.<T>toBuffer()).asJava();
  }

  /**
   * Convert a Scala {@link Option} to a Java {@link Optional}.
   *
   * @param opt
   * @param <T>
   * @return
   */
  public static <T> Optional<T> asJavaOptional(Option<T> opt) {
    return Optional.ofNullable(
        opt.getOrElse(
            new AbstractFunction0<T>() {
              @Override
              public T apply() {
                return null;
              }
            }));
  }
}
