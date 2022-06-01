# SPDX-License-Identifier: Apache-2.0

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,IntegerType
from pyspark.sql.functions import explode, from_json, col

os.makedirs("/tmp/delta", exist_ok=True)

spark = SparkSession.builder \
    .master("local") \
    .appName("Open Lineage Delta Save As Table") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.warehouse.dir", "file:/tmp/delta/") \
    .getOrCreate()
spark.sparkContext.setLogLevel('info')

df = spark.createDataFrame([
    '{"title":"Feeding Sea Lions","year":1900,"cast":["Paul Boyton"],"genres":[]}',
    '{"title":"The Wonder, Ching Ling Foo","year":1900,"cast":["Ching Ling Foo"],"genres":["Short"]}',
    '{"title":"Alice in Wonderland","year":1903,"cast":["May Clark"],"genres":[]}',
    '{"title":"Nicholas Nickleby","year":1903,"cast":["William Carrington"],"genres":[]}',
    '{"title":"The Automobile Thieves","year":1906,"cast":["J. Stuart Blackton","Florence Lawrence"],"genres":["Short","Crime","Drama"]}',
    '{"title":"Humorous Phases of Funny Faces","year":1906,"cast":["J. Stuart Blackton"],"genres":["Short","Animated","Animated"]}',
    '{"title":"Ben-Hur","year":1907,"cast":["William S. Hart"],"genres":["Historical"]}',
    '{"title":"Daniel Boone","year":1907,"cast":["William Craven","Florence Lawrence"],"genres":["Biography"]}',
    '{"title":"How Brown Saw the Baseball Game","year":1907,"cast":["Unknown"],"genres":["Comedy"]}',
    '{"title":"Laughing Gas","year":1907,"cast":["Bertha Regustus","Edward Boulden"],"genres":["Comedy"]}',
    '{"title":"The Adventures of Dollie","year":1908,"cast":["Arthur V. Johnson","Linda Arvidson"],"genres":["Drama"]}',
    '{"title":"Antony and Cleopatra","year":1908,"cast":["Florence Lawrence","William V. Ranous"],"genres":[]}',
    '{"title":"Balked at the Altar","year":1908,"cast":["Linda Arvidson","George Gebhardt"],"genres":["Comedy"]}',
    '{"title":"The Bandit\'s Waterloo","year":1908,"cast":["Charles Inslee","Linda Arvidson"],"genres":["Drama"]}',
    '{"title":"The Black Viper","year":1908,"cast":["D. W. Griffith"],"genres":["Drama"]}',
    '{"title":"A Calamitous Elopement","year":1908,"cast":["Harry Solter","Linda Arvidson"],"genres":["Comedy"]}',
    '{"title":"The Call of the Wild","year":1908,"cast":["Charles Inslee"],"genres":["Adventure"]}',
    '{"title":"A Christmas Carol","year":1908,"cast":["Tom Ricketts"],"genres":["Drama"]}',
    '{"title":"Deceived Slumming Party","year":1908,"cast":["Edward Dillon","D. W. Griffith","George Gebhardt"],"genres":["Comedy"]}',
    '{"title":"Dr. Jekyll and Mr. Hyde","year":1908,"cast":["Hobart Bosworth","Betty Harte"],"genres":["Horror"]}',
    '{"title":"The Fight for Freedom","year":1908,"cast":["Florence Auer","John G. Adolfi"],"genres":["Western"]}',
    '{"title":"Macbeth","year":1908,"cast":["William V. Ranous","Paul Panzer"],"genres":[]}',
    '{"title":"Money Mad","year":1908,"cast":["Charles Inslee","George Gebhardt"],"genres":["Crime"]}',
    '{"title":"The Red Man and the Child","year":1908,"cast":["Charles Inslee","John Tansey"],"genres":["Western"]}',
    '{"title":"Romeo and Juliet","year":1908,"cast":["Paul Panzer","Florence Lawrence"],"genres":[]}',
    '{"title":"The Taming of the Shrew","year":1908,"cast":["Florence Lawrence","Arthur V. Johnson"],"genres":[]}',
    '{"title":"The Tavern Keeper\'s Daughter","year":1908,"cast":["George Gebhardt","Florence Auer"],"genres":["Action"]}',
    '{"title":"And a Little Child Shall Lead Them","year":1909,"cast":["Marion Leonard","Arthur V. Johnson"],"genres":[]}',
    '{"title":"At the Altar","year":1909,"cast":["Marion Leonard"],"genres":["Drama"]}',
    '{"title":"The Brahma Diamond","year":1909,"cast":["Harry Solter","Florence Lawrence"],"genres":[]}',
    '{"title":"A Burglar\'s Mistake","year":1909,"cast":["Harry Solter","Charles Inslee"],"genres":[]}',
    '{"title":"The Cord of Life[1]","year":1909,"cast":["Charles Inslee","Marion Leonard"],"genres":[]}',
    '{"title":"The Criminal Hypnotist","year":1909,"cast":["Owen Moore","Marion Leonard"],"genres":[]}',
], StringType())
schema = StructType([
    StructField("title",StringType(),True),
    StructField("year", IntegerType(), True),
    StructField("cast",ArrayType(StringType()),True),
    StructField("genres",ArrayType(StringType()),True)
])

df.select(from_json("value", schema).alias("parsed")).select(col("parsed.*"))\
    .write.mode("overwrite").format("parquet")\
    .saveAsTable('movies')