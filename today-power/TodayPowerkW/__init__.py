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

def power_ts(**kw):
    device_id = ["mbm1","mbm2","mbm3","mbm4"]
    mbm = []
    for item in device_id:
            temp = query_cosmos_ts(
                device_id=item,
                start_time=kw['start_ts']-600, 
                end_time=kw['start_ts']
            )
            meter_data_start = []
            for i in range(len(temp[0]['meter_data'])):
                if i < 48:
                    meter_data_start.append(float(temp[0]['meter_data'][i]['value']))
                else:
                    break
            
            mbm.append(
                sum(meter_data_start)
            )
    return sum(mbm)        


def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')

    # now
    tz = timezone('Asia/Bangkok')
    utc_now = datetime.utcnow()
    now_element = pytz.utc.localize(utc_now, is_dst=None).astimezone(tz)
    now_ts = datetime.timestamp(now_element)
    
    # start_day
    today = now_element.strftime("%Y-%m-%d 00:00")
    today_element = datetime.strptime(today, "%Y-%m-%d %H:%M")
    start_ts = datetime.timestamp(today_element)

    # today_power_kW
    today_power_kW = []
    for item in container.query_items(
        query=f"SELECT * FROM c WHERE c.device_id = 'today_power_kW' and c.date = {start_ts} order by c.timestamp DESC",
        enable_cross_partition_query=True):
        today_power_kW.append(item)
    
    '''
    today_power_kW = query_cosmos_ts(
        device_id='today_power_kW',
        start_time=now_ts-600, # 10 minutes ago
        end_time=now_ts # now
    )
    '''
    
    if not today_power_kW:
        
        now_power = power_ts(start_ts=now_ts)
        
        container.upsert_item({
            "device_id": "today_power_kW",
            "timestamp": int(now_ts),
            "date": int(start_ts),
            'data': [
                {
                    "timestamp": int(now_ts),
                    "value": now_power/1000
                }
            ]
        })
    
    else:
        now_power = power_ts(start_ts=now_ts)
    
        if int(today_power_kW[0]['date']) != int(start_ts):
        
            container.upsert_item({
                "device_id": "today_power_kW",
                "timestamp": int(now_ts),
                "date": int(start_ts),
                'data': [
                    {
                        "timestamp": int(now_ts),
                        "value": now_power/1000
                    }
                ]
            })
        
        
        else:
            today_power_kW[0]['data'].append(
                {
                    "timestamp": int(now_ts),
                    "value": now_power/1000
                }
            )
            today_power_kW[0]['timestamp'] = int(now_ts)
            container.upsert_item(today_power_kW[0])
