



"""
 * This program launches a spark streaming server which monitors Kafka topic mentioned above
 * for incoming numbers. It then calculates average sentiment using a rolling window
 * approach
"""
from kafka import KafkaConsumer,KafkaProducer
from kafka.errors import KafkaError
from pyspark.sql.functions import *
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
class Average(object):
    conf=SparkConf()\
        .setAppName("avg_sentiment")\
        .set("spark.sql.shuffle.partitions","4")

    spark=SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()


    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sentiment") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "latest") \
        .load()

    dateFormat = "yyyy-MM-dd HH:mm:ss"
    readingSchema = "time string, sentiment double,group string"

    result1 = stream \
        .selectExpr("CAST(value AS string) AS val") \
        .select(from_json("val", readingSchema).alias("v")) \
        .select(expr("CAST(v.time as timestamp)"), expr("v.sentiment"), expr("v.group")) \
        .withWatermark("time", "10 seconds") \
        .groupBy(window("time", windowDuration="2 seconds",slideDuration="2 seconds")) \
        .agg(avg("sentiment").alias("average")) \
        .selectExpr("CAST(average AS string) AS value")
        # .select(struct("average").alias("value"))


        # .select(to_json("x").alias("value"))

        # .selectExpr("average as result") \
        # .select(to_json("result").alias("value"))

# .selectExpr("""(date_format(window.start,'{}') as start,date_format(window.end,'{}') as end,average) as result""" \
#         .format(dateFormat, dateFormat)) \

    query1 = result1.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("topic", "average") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()

    query2 = result1.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    spark.streams.awaitAnyTermination()

    spark.stop()


    # result = stream \
    #     .selectExpr("CAST(value AS string) AS val") \
    #     .select(from_json("val", readingSchema).alias("v")) \
    #     .select(expr("CAST(v.time AS timestamp)"), expr("v.sentiment AS sent"), expr("v.group")) \
    #     .withWatermark("time", "10 seconds")\
    #     .groupBy("group", window("time", windowDuration="20 seconds")) \
    #     .agg(avg("sent").alias("average")) \
    #     .selectExpr("(average) as result") \
    #     .select(struct("result").alias("x"))\
    #     .select(to_json("x").alias("value"))


    # result=stream\
    #     .selectExpr("CAST(value AS string) AS val")\
    #     .select(from_json("val",readingSchema).alias("v"))\
    #     .select(expr("CAST(v.time AS timestamp)"),expr("v.sentiment"),expr("v.group"))\
    #     .withWatermark("time", "10 seconds") \
    #     .groupBy("group", window("time", windowDuration="20 seconds")) \
    #     .agg(avg("sentiment").alias("average")) \
    #     .selectExpr("(sentiment, date_format(window.start, '{}') as start, date_format(window.end, '{}') as end, average) as result".format(
    #         dateFormat, dateFormat)) \
    #     .select(to_json("result").alias("value"))
    #
    # query2 = result.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    # result1= stream \
    #     .selectExpr("CAST(value AS string) AS val") \
    #     .select(from_json("val",readingSchema).alias("v")) \
    #     .select(expr("CAST(v.time as timestamp)"),expr("v.sentiment"),expr("v.group")) \
    #     .withWatermark("time","5 seconds") \
    #     .groupBy("group",window("time",windowDuration="2 seconds",slideDuration="2 seconds")) \
    #     .agg(avg("sentiment").alias("average")) \
    #     .selectExpr("""(group,date_format(window.start,'{}') as start,date_format(window.end,'{}') as end,average) as result""".format(dateFormat,dateFormat)) \
    #     .select(to_json("result").alias("value"))

    # query1.awaitTermination()
    # query2.awaitTermination()



