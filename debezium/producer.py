import psycopg2
import time

conn = psycopg2.connect(
  database="postgres", user='postgres', password='postgres', host='192.168.56.64', port= '30810'
)
cursor = conn.cursor()

query = "INSERT INTO public.test_table (id, name) VALUES "

for x in range(100, 2000):
    time.sleep(0.2)
    cursor.execute(query + f"({x},{'tuan' + str(x)})" + ";")
        

conn.commit()