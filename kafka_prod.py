from kafka import KafkaProducer
import json
import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:29092'], 
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
topic = 'k8s_1'

msg = {
    "time": str(datetime.datetime.now())
}

producer.send(topic, value=msg)
producer.flush()
print(msg)