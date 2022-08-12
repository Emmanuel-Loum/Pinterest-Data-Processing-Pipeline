import os
import multiprocessing
from tokenize import String
import findspark
findspark.init
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import pandas as pd
from pyspark.sql.functions import translate
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql.types import StructType,StructField ,StringType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
import json
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace
import numpy as np


#store the jar files where pyspark jars are
os.environ['PYSPARK_SUBMIT_ARGS'] ='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell org.postgresql:postgresql-42.4.1 pyspark-shell'

kafka_topic_name = "coretopic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()


stream_df = stream_df.selectExpr("CAST(value as STRING)")

#create a schema for the dataframe
schema =StructType([
        StructField("index",IntegerType(),False),
        StructField("unique_id",StringType(),False),
        StructField("title",StringType(),True),
        StructField("description",StringType(),True),
        StructField("poster_name",StringType(),True),
        StructField("follower_count",StringType(),True),
        StructField("tag_list",StringType(),True),
        StructField("is_image_or_video",StringType(),True),
        StructField("image_src",StringType(),True),
        StructField("downloaded",StringType(),True),
        StructField("save_location",StringType(),True),
        StructField("category",StringType(),True)])



stream_rdf = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

def save_to_postgres(df,epoch):
        #Perform your transformation 
        df = df.withColumn('follower_count',when(df.follower_count.endswith('k'),regexp_replace(df.follower_count,'k','000')) \
                .when(df.follower_count.endswith('M'),regexp_replace(df.follower_count,'M','000000')) \
                .when(df.follower_count.endswith('B'),regexp_replace(df.follower_count,'B','000000000'))\
                .otherwise(df.follower_count))
        #change follower_count column to int data type
        df = df.withColumn("follower_count", df["follower_count"].cast(IntegerType())).fillna(0) \
                        .drop("index")
        
        #send data to postgres
        df.write.format("jdbc")\
                .option("url", "jdbc:postgresql://localhost:5432/pinterest_streaming") \
                .option("driver", "org.postgresql.Driver").option("dbtable", "experimental_data") \
                .option("user", "postgres").option("password", "postgres").mode("append").save()
            
stream_rdf = stream_rdf.writeStream \
        .format("console") \
        .outputMode("append") \
        .foreachBatch(save_to_postgres)\
        .start() \
        .awaitTermination()