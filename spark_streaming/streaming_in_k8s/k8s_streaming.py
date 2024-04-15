from pyspark.sql import SparkSession                                          
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json, explode, col, current_date
import os                     
from dotenv import load_dotenv
             
load_dotenv()
                                              
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')        
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
                                      
KAFKA_HOST1 = os.getenv('KAFKA_HOST1')
KAFKA_HOST2 = os.getenv('KAFKA_HOST2')
KAFKA_HOST3 = os.getenv('KAFKA_HOST3')              
KAFKA_BROKER1_PORT = os.getenv('KAFKA_BROKER1_PORT')
KAFKA_BROKER2_PORT = os.getenv('KAFKA_BROKER2_PORT')
KAFKA_BROKER3_PORT = os.getenv('KAFKA_BROKER3_PORT')
                          
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
        .option("url", "jdbc:clickhouse://" + CLICKHOUSE_HOST + ":" + CLICKHOUSE_PORT) \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("dbtable", "dateam.raw_url") \
        .save()
                          
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("spark://spark-master-7d99674d56-vzhjx:7077") \
        .config("spark.driver.bindAddress", "10.10.19.159") \
        .config("spark.driver.host", "10.10.19.159") \
        .getOrCreate()                    
                                                  
    # failOnDataLoss: https://stackoverflow.com/questions/64922560/pyspark-and-kafka-set-are-gone-some-data-may-have-been-missed
    print(KAFKA_HOST1)                                                                      
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST1 + ":" + KAFKA_BROKER1_PORT + "," + KAFKA_HOST2 + ":" + KAFKA_BROKER2_PORT + "," + KAFKA_HOST3 + ":" + KAFKA_BROKER3_PORT) \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "test-url-1504") \
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