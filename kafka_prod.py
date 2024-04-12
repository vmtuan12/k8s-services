from kafka import KafkaProducer
import json
import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'], 
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
topic = 'replication-3'

msg = {
    "time": str(datetime.datetime.now())
}

url_msg = {
    "sslsni": "fDjgKlI",
    "subscriberid": "eRgHbZ",
    "hour_key": str(datetime.datetime.now()),
    "count": "37",
    "up": "25",
    "down": "12"
}

producer.send(topic, value=url_msg)
producer.flush()
print(url_msg)