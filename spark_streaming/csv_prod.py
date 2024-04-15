from kafka import KafkaProducer
import json
import pandas as pd
import re

phone_number_regex = r"^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$"

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
    try:
        if any(pd.isna(x) for x in row):
            continue

        if re.search(phone_number_regex, row['subscriberid'], re.IGNORECASE | re.MULTILINE):
            url_msg = {
                "sslsni": row['sslsni'],
                "subscriberid": row['subscriberid'],
                "hour_key": int(row['hour_key']),
                "count": int(row['count(1)']),
                "up": int(row['UP']),
                "down": int(row['DOWN'])
            }

            producer.send(topic, value=url_msg)
            producer.flush()
            print(url_msg)
        
    except:
        # add more logic here to control error
        continue