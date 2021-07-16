from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Open Lineage Integration Word Count").getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.read.text("/test_data/data.txt")

agg = df.groupBy("value").count()
agg.write.mode("overwrite").csv("/test_data/test_output/")