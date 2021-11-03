from pyspark.sql import SparkSession

appName = "Kafka Examples"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

kafka_servers = "kafka:9092"
checkpointLocation = '/test_data/checkpoint'


people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])

ds = people \
    .selectExpr("CAST(name AS STRING) as key", "CAST(age AS STRING) as value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("topic", "topic1") \
    .option("checkpointLocation", checkpointLocation) \
    .save()

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "topic1") \
    .load()

df.show()
