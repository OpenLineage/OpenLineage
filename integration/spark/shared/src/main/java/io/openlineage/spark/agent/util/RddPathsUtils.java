/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Arrays;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.package$;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.execution.datasources.FileScanRDD;

/** Utility class to extract paths from RDD nodes. */
@Slf4j
public class RddPathsUtils {

  public static Stream<Path> findRDDPaths(RDD rdd) {
    return Stream.<RddPathExtractor>of(
            new HadoopRDDExtractor(),
            new FileScanRDDExtractor(),
            new MapPartitionsRDDExtractor(),
            new ParallelCollectionRDDExtractor())
        .filter(e -> e.isDefinedAt(rdd))
        .findFirst()
        .orElse(new UnknownRDDExtractor())
        .extract(rdd)
        .filter(p -> p != null);
  }

  static class UnknownRDDExtractor implements RddPathExtractor<RDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return true;
    }

    @Override
    public Stream<Path> extract(RDD rdd) {
      log.warn("Unknown RDD class {}", rdd.getClass().getCanonicalName());
      return Stream.empty();
    }
  }

  static class HadoopRDDExtractor implements RddPathExtractor<HadoopRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof HadoopRDD;
    }

    @Override
    public Stream<Path> extract(HadoopRDD rdd) {
      org.apache.hadoop.fs.Path[] inputPaths = FileInputFormat.getInputPaths(rdd.getJobConf());
      Configuration hadoopConf = rdd.getConf();
      return Arrays.stream(inputPaths).map(p -> PlanUtils.getDirectoryPath(p, hadoopConf));
    }
  }

  static class MapPartitionsRDDExtractor implements RddPathExtractor<MapPartitionsRDD> {

    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof MapPartitionsRDD;
    }

    @Override
    public Stream<Path> extract(MapPartitionsRDD rdd) {
      return findRDDPaths(rdd.prev());
    }
  }

  static class FileScanRDDExtractor implements RddPathExtractor<FileScanRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof FileScanRDD;
    }

    @Override
    public Stream<Path> extract(FileScanRDD rdd) {
      return ScalaConversionUtils.fromSeq(rdd.filePartitions()).stream()
          .flatMap(fp -> Arrays.stream(fp.files()))
          .map(
              f -> {
                if (package$.MODULE$.SPARK_VERSION().compareTo("3.4") > 0) {
                  // filePath returns SparkPath for Spark 3.4
                  return ReflectionUtils.tryExecuteMethod(f, "filePath")
                      .map(o -> ReflectionUtils.tryExecuteMethod(o, "toPath"))
                      .map(o -> (Path) o.get())
                      .get()
                      .getParent();
                } else {
                  return parentOf(f.filePath());
                }
              });
    }
  }

  static class ParallelCollectionRDDExtractor implements RddPathExtractor<ParallelCollectionRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof ParallelCollectionRDD;
    }

    @Override
    public Stream<Path> extract(ParallelCollectionRDD rdd) {
      log.debug("extracting RDD paths from ParallelCollectionRDD");
      return Arrays.stream(rdd.getPartitions())
          .flatMap(
              partition ->
                  ScalaConversionUtils.<String>fromSeq(rdd.getPreferredLocations(partition))
                      .stream())
          .map(location -> parentOf((String) location));
    }
  }

  private static Path parentOf(String path) {
    try {
      return new Path(path).getParent();
    } catch (Exception e) {
      return null;
    }
  }

  interface RddPathExtractor<T extends RDD> {
    boolean isDefinedAt(Object rdd);

    Stream<Path> extract(T rdd);
  }
}
