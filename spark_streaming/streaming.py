from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import from_json, explode, col

json_schema = StructType([
    StructField('sslsni', StringType(), True),
    StructField('subscriberid', StringType(), True),
    StructField('hour_key', StringType(), True),
    StructField('count', StringType(), True),
    StructField('up', StringType(), True),
    StructField('down', StringType(), True)
])

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("spark://mhtuan-HP:7077") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093") \
    .option("subscribe", "test-url-1204") \
    .load()
json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")
df.printSchema()

json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")

exploded_df = json_expanded_df.select("sslsni", "subscriberid", "hour_key", "count", "up", "down") 

writing_df = exploded_df \
    .writeStream \
    .format("console") \
    .option("checkpointLocation","checkpoint_dir") \
    .outputMode("append") \
    .start()

writing_df.awaitTermination()