from pyspark.sql import SparkSession
import config

def read_from_kafka():

    # parameteres
    appName = config.spark["appName"]
    server = config.kafka["bootstrap_servers"]
    topic = config.kafka["topic"]

    spark = SparkSession.builder \
        .appName(appName) \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", server) \
        .option("subscribe", topic) \
        .load()

    kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_data")

    return spark, kafka_df
