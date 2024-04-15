# Spark streaming

This folder is for submitting application in K8s

```
./spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.github.housepower:clickhouse-integration-spark_2.12:2.7.1,com.github.housepower:clickhouse-native-jdbc-shaded:2.7.1 home/test_streaming.py
```