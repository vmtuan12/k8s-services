from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'], 
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
topic = 'test-url-1204'

df = pd.read_csv('mdo_tcp.20231231.03', dtype= {
    'sslsni': str,
    'subscriberid': str,
    'hour_key': str,
    'count(1)': str,
    'UP': str,
    'DOWN': str,
})

for index, row in df.iterrows():
    url_msg = {
        "sslsni": row['sslsni'],
        "subscriberid": row['subscriberid'],
        "hour_key": row['hour_key'],
        "count": row['count(1)'],
        "up": row['UP'],
        "down": row['DOWN']
    }

    producer.send(topic, value=url_msg)
    producer.flush()
    print(url_msg)