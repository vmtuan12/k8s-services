from kafka import KafkaConsumer, TopicPartition
from kafka.cluster import ClusterMetadata
import time

def fetch_data():
    consumer = KafkaConsumer('topic-rep-3',
                            group_id='group-test',
                            bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'])
    
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value))

fetch_data()