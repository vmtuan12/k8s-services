import psycopg2
from kafka import KafkaConsumer
import json
import time

conn = psycopg2.connect(
  database="postgres", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
)
cursor = conn.cursor()

query = "INSERT INTO public.test_table (id, name) VALUES "
batch = []
count = 0
start = None

def fetch_data():
    consumer = KafkaConsumer('postgre.public.test_table',
                            group_id='group-test',
                            bootstrap_servers=["192.168.56.61:9094","192.168.56.62:9094","192.168.56.63:9094"])
    
    global count
    global batch

    for message in consumer:
        dict_msg = json.loads(message.value.decode('utf-8'))

        value = dict_msg["after"]

        # cursor.execute(query + ("(" + str(value['id']) + ",'" + str(value["name"]) + "')") + ";")
        # conn.commit()

        if len(batch) == 0:
            global start
            start = time.time()

        batch.append("(" + str(value['id']) + ",'" + str(value["name"]) + "')")

        if len(batch) == 1000:
            cursor.execute(query + ', '.join(batch) + ";")
            conn.commit()
            consumer.commit()
            print(time.time() - start)
            batch = []

fetch_data()