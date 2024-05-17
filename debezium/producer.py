import psycopg2
import time

conn = psycopg2.connect(
  database="postgres", user='postgres', password='postgres', host='192.168.56.64', port= '30810'
)
cursor = conn.cursor()

query = "INSERT INTO public.test_table (id, name) VALUES "

list_batch = []
start = 160000
while (start < 2500000):
    list_batch.append([("(" + str(x) + ",'" + "tuan-" + str(x) + "')") for x in range(start, start + 1000)])
    start += 1000


for item in list_batch:
    cursor.execute(query + ', '.join(item) + ";")
    conn.commit()