import logging

import azure.functions as func
from azure.cosmos import CosmosClient
from datetime import datetime, timedelta
from pytz import timezone
import pytz
import psycopg2

url = "https://altolido.documents.azure.com:443/"
key = "d1ebjRT7a5n3k7fyHSQM2TJRVb5lxHYor7xgyW6skWkKiEge2z6HMGSTiIg6YNJo7D7mt1ZryAgiT6fZv8rcEg=="
client = CosmosClient(url, credential=key)
database_name = 'iotdata'
database = client.get_database_client(database_name)
container_name = 'lido'
container = database.get_container_client(container_name)

# Update connection string information
host = "altoiotmonitor.postgres.database.azure.com"
dbname = "postgres"
user = "altoiotmonitor@altoiotmonitor"
password = "Magicalmint636"
#sslmode = "require"


def electricity_rate():
    # Construct connection string
    conn_string = "host={0} user={1} dbname={2} password={3}".format(host, user, dbname, password)
    conn = psycopg2.connect(conn_string)
    print("Connection established")
    cursor = conn.cursor()
    # Fetch all rows from table
    cursor.execute("SELECT electricity_rate FROM settings WHERE building_id = 1;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows[0][0]

def query_cosmos_ts(**kw):
    
    query_string=f"SELECT * from c where\
        c.device_id = '{kw['device_id']}' \
        and c.timestamp >= {kw['start_time']}\
        and c.timestamp <= {kw['end_time']} \
        order by c.timestamp DESC"
    

    res = []
    for item in container.query_items(
            query=query_string,
            enable_cross_partition_query=True):
        res.append(item)
    
    return res


def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info('Python HTTP trigger function processed a request.')
    tz = timezone('Asia/Bangkok')
    utc_now = datetime.utcnow()
    now_element = pytz.utc.localize(utc_now, is_dst=None).astimezone(tz)
    now_ts = datetime.timestamp(now_element)
    logging.info(f"utc now: {utc_now}")
    logging.info(f"now timezone {tz}: {now_element}")
    logging.info(f"now timestamp timezone {tz}: {now_ts}")

    today = now_element.strftime("%Y-%m-%d 00:00")
    today_element = datetime.strptime(today, "%Y-%m-%d %H:%M")
    date = datetime.timestamp(today_element)
    date = int(date) - (60*60*7)
    logging.info(f"today: {today}")
    logging.info(f"today timezone {tz}: {today_element}")
    logging.info(f"today timestamp timezone {tz}: {date}")


    lastMonth = today_element - timedelta(days=1)
    lastMonth = lastMonth.replace(day=1)
    lastMonth_ts = datetime.timestamp(lastMonth)
    logging.info(f"today: {today}")
    logging.info(f"today timezone {tz}: {today_element}")
    logging.info(f"today timestamp timezone {tz}: {date}")
    logging.info(f"last month: {lastMonth}")
    logging.info(f"last month timestamp: {lastMonth_ts}")

    device_id = ["mbm1","mbm2","mbm3","mbm4"]
    point = []
    ebill_rate = 7 #bath/kWh
    for item in device_id:
            start_data = query_cosmos_ts(
                device_id=item,
                start_time=int(lastMonth_ts), 
                end_time=int(lastMonth_ts)+600
            )
            now_data = query_cosmos_ts(
                device_id=item,
                start_time=int(now_ts)-600, 
                end_time=int(now_ts)
            )
            
            for i in range(len(now_data[0]['meter_data'])):
                if i < 48:
                    energy_instance = float(now_data[0]['meter_data'][i+48]['value'])-float(start_data[-1]['meter_data'][i+48]['value'])
                    point.append({
                        "mbm":item,
                        "ct":str(i+1),
                        "power_kW":float(now_data[0]['meter_data'][i]['value'])/1000,
                        "energy_kW":energy_instance/1000,
                        "energy_kWh":energy_instance/1000,
                        "ebill":(energy_instance*ebill_rate)/1000
                    })
                else:
                    break
    ret = {
        "device_id":"monthly",
        "timestamp":int(now_ts),
        "datetime":str(now_element),
        "gatewayid": "lidombmmonitor",
        "point":point
    }
    container.upsert_item(ret)