from pyspark.sql import SparkSession

appName = "Kafka Examples"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

kafka_servers = "kafka:9092"
checkpointLocation = '/test_data/checkpoint'

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "topic1") \
    .load()

ds = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("topic", "topic2") \
    .option("checkpointLocation", checkpointLocation) \
    .save()
