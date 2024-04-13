from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json, explode, col, current_date
from datetime import date

json_schema = StructType([
    StructField('sslsni', StringType(), True),
    StructField('subscriberid', StringType(), True),
    StructField('hour_key', IntegerType(), True),
    StructField('count', IntegerType(), True),
    StructField('up', IntegerType(), True),
    StructField('down', IntegerType(), True)
])

def foreach_batch_function(df, epoch_id):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://127.0.0.1:9000") \
        .option("user", "default") \
        .option("password", "tuan281203") \
        .option("dbtable", "default.raw") \
        .save()

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("spark://mhtuan-HP:7077") \
        .getOrCreate()

    # failOnDataLoss: https://stackoverflow.com/questions/64922560/pyspark-and-kafka-set-are-gone-some-data-may-have-been-missed
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "test-url-1204") \
        .load()
    json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")
    df.printSchema()

    json_expanded_df = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")

    exploded_df = json_expanded_df.select("sslsni", "subscriberid", "hour_key", "count", "up", "down") 

    df_with_date = exploded_df.withColumn("date", current_date())

    writing_df = df_with_date \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .start()
    
    # writing_df = df_with_date \
    #     .writeStream \
    #     .format("console") \
    #     .option("checkpointLocation","checkpoint_dir") \
    #     .outputMode("append") \
    #     .start()

    writing_df.awaitTermination()