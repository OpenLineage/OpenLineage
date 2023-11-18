from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = (SparkSession.builder.master('local')
         .appName('etl-test')
         .config('spark.jars.packages', "io.openlineage:openlineage-spark:1.1.0,mysql:mysql-connector-java:8.0.33")
         .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener')
         .config('spark.openlineage.transport.type', 'http')
         .config('spark.openlineage.transport.url', 'http://host.docker.internal:5009/events/spark/')
         .config('spark.openlineage.namespace', 'staging')
         .config('spark.openlineage.transport.auth.type', 'api_key')
         .config('spark.openlineage.transport.auth.apiKey', 'abcdefghijk')
         .getOrCreate())


spark.sparkContext.setLogLevel("INFO")


# Define schema for JSON data
schema = StructType([
  StructField("customer_id", IntegerType(), True),
  StructField("order_id", IntegerType(), True),
  StructField("order_date", StringType(), True),
  StructField("order_amount", StringType(), True),
  StructField("product_name", StringType(), True)
])


# Read JSON data into PySpark DataFrame
df = spark.read.option('multiline', True).json("input.json", schema=schema)

# Apply data transformations
df_transformed = df.select(
  col("customer_id"),
  year(col("order_date")).alias("order_year"),
  month(col("order_date")).alias("order_month"),
  dayofmonth(col("order_date")).alias("order_day"),
  col("order_amount").cast("float").alias("order_amount"),
  concat_ws("-", col("customer_id"), col("order_id")).alias("order_key"),
  col("product_name")
)


# Write transformed data to MySQL database
url = "<url>"
table_name = "orders"
mode = "append"
properties = {
  "driver": "com.mysql.cj.jdbc.Driver",
  "user": "<user>",
  "password": "<pwd>"
}


df_transformed.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)