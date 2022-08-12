import findspark
import multiprocessing

import pyspark
from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
findspark.init()


# We should always start with session in order to obtain
# context and session if needed
session = pyspark.sql.SparkSession.builder.config(
    conf=pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("TestApp")
).getOrCreate()

# This context can be used with PySpark streaming
# You might have to specify batchDuration (e.g. on which time window operation will be run)
# By default data is collected every 0.5 seconds
ssc = StreamingContext(session.sparkContext, batchDuration=30)

# We will send lines of data to this socketTextStream
lines = ssc.socketTextStream("localhost",9999)
#unique_words = lines.flatMap(lambda text: text.split()).countByValue()
#unique_words = lines.flatMap(lambda text: text.split()).countByValue()
#unique_words.pprint()
lines.pprint()
ssc.start()
seconds = 180
ssc.awaitTermination(seconds)
ssc.stop(stopGraceFully=True)