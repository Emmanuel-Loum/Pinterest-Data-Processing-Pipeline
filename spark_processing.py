import multiprocessing
from os import truncate
import boto3
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from collections import Counter
from pyspark import SparkContext
from pyspark import SparkConf
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import sys
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import IntegerType

class Spark:

    def __init__(self): 
        pass

    def s3_extract(self):


        s3 = boto3.resource('s3')
        s3D = boto3.client('s3')
        my_bucket = s3.Bucket('s3courier')
        latest_num=len(Counter(my_bucket.objects.all()))
        ln=latest_num-1

        for n,file in enumerate(my_bucket.objects.all()):
            print(n,file)
            if n == ln:
                #Of course, change the names of the files to match your 
                s3D.download_file('s3courier', f'{file.key}', 'data.json')

    def write_to_cassandra(self):

        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 spark_processing.py pyspark-shell '

        sparkConf=SparkConf()
        sc = SparkContext(conf=sparkConf)
        spark = SparkSession.builder \
        .appName('SparkCassandraApp') \
        .getOrCreate()

        df = spark.read.option('multiline','true').json("data.json")

        print("Distinct count: "+str(df.count()))
        df = df.dropDuplicates()
        print("Distinct count: "+str(df.count()))
        df = df.withColumn('follower_count',when(df.follower_count.endswith('k'),regexp_replace(df.follower_count,'k','000')) \
                .when(df.follower_count.endswith('M'),regexp_replace(df.follower_count,'M','000000')) \
                .when(df.follower_count.endswith('B'),regexp_replace(df.follower_count,'B','000000000'))\
                .otherwise(df.follower_count))
        #change follower_count column to int data type
        df = df.withColumn("follower_count", df["follower_count"].cast(IntegerType())).fillna(0) \
                        .drop("index")

        
        df.printSchema()
        df.show(10)
        #remember to replace the 2k to 2000 and integer format
        df.write.format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="pindata", keyspace="spark_keyspace")\
            .save()
        
        print("===========================Data sent to Cassandra successfully========================================")
    
        spark.stop
        

Spark().s3_extract()
Spark().write_to_cassandra()










