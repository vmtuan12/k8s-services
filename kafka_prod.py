from kafka import KafkaProducer
import json
import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'], 
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
topic = 'rep-3'

msg = {
    "time": str(datetime.datetime.now())
}

producer.send(topic, value=msg)
producer.flush()
print(msg)