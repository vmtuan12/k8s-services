import clickhouse_connect
from pprint import pprint

latest_record_time = None
where_condition = ''

client = clickhouse_connect.get_client(host='localhost', username='default', password='tuan281203')

def get_array_result(query: str) -> list:
    print(query)
    query_result = client.query(query)
    rows = query_result.result_rows
    return [list(t) for t in rows]

def save_max_inserted_time():
    max_inserted_time = client.query(f"SELECT toString(max(inserted_time)) FROM raw_url WHERE inserted_time > '{latest_record_time}'").result_rows[0][0]
    if max_inserted_time != '1970-01-01 08:00:00.000':
        record_time_log = open("record_time.log", "w+")
        record_time_log.write(max_inserted_time)
        record_time_log.close()

if __name__ == '__main__':

    try:
        record_time_log = open('record_time.log', 'r')
        latest_record_time = record_time_log.read()
        record_time_log.close()
    except:
        latest_record_time = '1970-01-01 00:00:00.000'

    arr_rows = get_array_result(f"SELECT subscriberid, sslsni, up, down, [hour_key], [count] FROM raw_url WHERE inserted_time > '{latest_record_time}'")
    
    if latest_record_time != '1970-01-01 00:00:00.000':
        # consider deleting old records
        arr_expired_rows = get_array_result(f"SELECT subscriberid, sslsni, up, down, [hour_key], [-count] \
                                            FROM raw_url WHERE inserted_time <= toDateTime(date_sub(DAY, 6, toDate('{latest_record_time}')))")
        arr_rows += arr_expired_rows

    client.insert('top_url', arr_rows, column_names=['subscriberid', 'sslsni', 'total_up', 'total_down', 'frequent_hour_key_Map.hour_key', 'frequent_hour_key_Map.count'])
    client.command("OPTIMIZE TABLE top_url FINAL;")

    client.query("ALTER TABLE top_url DELETE WHERE arrayReduce('min', frequent_hour_key_Map.count) <= 0")

    save_max_inserted_time()