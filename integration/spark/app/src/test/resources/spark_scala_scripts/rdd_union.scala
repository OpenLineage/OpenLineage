{
  import spark.implicits._
  import org.apache.spark.sql.SaveMode

  sc
    .parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    .map(a => a.toString())
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet("/tmp/scala-test/rdd_input1")

  sc
    .parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    .map(a => a.toString())
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet("/tmp/scala-test/rdd_input2")

  val rdd1 = spark.read.parquet("/tmp/scala-test/rdd_input1").rdd
  val rdd2 = spark.read.parquet("/tmp/scala-test/rdd_input2").rdd

  rdd1
    .union(rdd2)
    .map(i => OLC(i.toString))
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet("/tmp/scala-test/rdd_output")
}

case class OLC(payload: String)