import logging

import azure.functions as func
from azure.cosmos import CosmosClient
from datetime import datetime
from pytz import timezone
import pytz

url = "https://altolido.documents.azure.com:443/"
key = "d1ebjRT7a5n3k7fyHSQM2TJRVb5lxHYor7xgyW6skWkKiEge2z6HMGSTiIg6YNJo7D7mt1ZryAgiT6fZv8rcEg=="
client = CosmosClient(url, credential=key)
database_name = 'iotdata'
database = client.get_database_client(database_name)
container_name = 'lido'
container = database.get_container_client(container_name)

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
    #today_element = pytz.utc.localize(today_element, is_dst=None).astimezone(tz)
    date = datetime.timestamp(today_element)
    month = now_element.strftime("%Y-%m-1 00:00")
    month_element = datetime.strptime(month, "%Y-%m-%d %H:%M")
    #today_element = pytz.utc.localize(today_element, is_dst=None).astimezone(tz)
    month_ts = datetime.timestamp(month_element)
    logging.info(f"today: {today}")
    logging.info(f"today timezone {tz}: {today_element}")
    logging.info(f"today timestamp timezone {tz}: {date}")

    device_id = ["mbm1","mbm2","mbm3","mbm4"]
    point = []
    ebill_rate = 7 #bath/kWh
    for item in device_id:
            start_data = query_cosmos_ts(
                device_id=item,
                start_time=int(date), 
                end_time=int(date)+600
            )
            start_month = query_cosmos_ts(
                device_id=item,
                start_time=int(month_ts), 
                end_time=int(month_ts)+600
            )
            now_data = query_cosmos_ts(
                device_id=item,
                start_time=int(now_ts)-600, 
                end_time=int(now_ts)
            )
            
            for i in range(len(now_data[0]['meter_data'])):
                if i < 48:
                    energy_instance = float(now_data[0]['meter_data'][i+48]['value'])-float(start_data[-1]['meter_data'][i+48]['value'])
                    monthly_energy_kW = float(now_data[0]['meter_data'][i+48]['value'])-float(start_month[-1]['meter_data'][i+48]['value'])
                    point.append({
                        "mbm":item,
                        "ct":str(i+1),
                        "power_kW":float(now_data[0]['meter_data'][i]['value'])/1000,
                        "energy_kW":energy_instance/1000,
                        "ebill":(energy_instance*ebill_rate)/1000,
                        "monthly_energy_kW":monthly_energy_kW/1000
                    })
                else:
                    break
    ret = {
        "device_id":"hourly",
        "timestamp":int(now_ts),
        "datetime":str(now_element),
        "point":point
    }
    container.upsert_item(ret)