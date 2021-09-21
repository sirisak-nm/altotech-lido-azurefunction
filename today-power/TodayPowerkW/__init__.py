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
        order by c.timestamp DESC OFFSET 0 LIMIT 1"
    

    res = []
    for item in container.query_items(
            query=query_string,
            enable_cross_partition_query=True):
        res.append(item)
    
    return res

def query_power_5min(**kw):
    time_range = kw['start_time']-(60*10) # 10 minutes => 10 point
    query_string=f"SELECT * from c where\
        c.device_id = '{kw['device_id']}' \
        and c.timestamp >= {time_range}\
        and c.timestamp <= {kw['start_time']} \
        order by c.timestamp DESC OFFSET 0 LIMIT 5"
    

    res = []
    for item in container.query_items(
            query=query_string,
            enable_cross_partition_query=True):
        res.append(item)
    
    return res

def avg_power_ts(**kw):
    device_id = ["mbm1","mbm2","mbm3","mbm4"]
    mbm = []
    time_range = kw['start_ts']-(60*10)
    for item in device_id:
        query_string=f"SELECT * from c where\
        c.device_id = '{item}' \
        and c.timestamp >= {time_range}\
        and c.timestamp <= {kw['start_ts']} \
        order by c.timestamp DESC OFFSET 0 LIMIT 5"
        temp = []
        for element in container.query_items(
            query=query_string,
            enable_cross_partition_query=True):
            temp.append(element)
        meter_data = []
        for i in range(len(temp[0]['meter_data'])):
            if i < 48:
                avg_W = sum([float(avg['meter_data'][i]['value']) for avg in temp])/len(temp)
                meter_data.append(avg_W)
            else:
                break
            
        mbm.append(
            sum(meter_data)/48
        )
    return sum(mbm)/4 

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
    start_ts = int(start_ts) - (60*60*7)

    # today_power_kW
    today_power_kW = []
    for item in container.query_items(
        query=f"SELECT * FROM c WHERE c.device_id = 'today_power_kW' order by c.timestamp DESC OFFSET 0 LIMIT 1",
        enable_cross_partition_query=True):
        today_power_kW.append(item)
    logging.info(f'{today_power_kW}')
    
    '''
    today_power_kW = query_cosmos_ts(
        device_id='today_power_kW',
        start_time=now_ts-600, # 10 minutes ago
        end_time=now_ts # now
    )
    '''
    
    if not today_power_kW:
        
        now_power = power_ts(start_ts=now_ts)
        now_avg_power = avg_power_ts(start_ts=now_ts)
        logging.info(f'now power {now_avg_power}')
        
        container.upsert_item({
            "device_id": "today_power_kW",
            "timestamp": int(now_ts),
            "datetime":str(now_element),
            "today_datetime": today_element.strftime("%Y-%m-%d"),
            "today_timestamp": int(start_ts),
            "gatewayid": "lidombmmonitor",
            'data': [
                {
                    "timestamp": int(now_ts),
                    "today_kW":now_avg_power/1000 #5 minutes ago power (Energy/1000)
                }
            ]
        })
    
    else:
        now_power = power_ts(start_ts=now_ts)
        now_avg_power = avg_power_ts(start_ts=now_ts)
        
    
        if int(today_power_kW[0]['today_timestamp']) != int(start_ts):
        
            container.upsert_item({
                "device_id": "today_power_kW",
                "timestamp": int(now_ts),
                "datetime":str(now_element),
                "today_datetime": today_element.strftime("%Y-%m-%d"),
                "today_timestamp": int(start_ts),
                "gatewayid": "lidombmmonitor",
                'data': [
                    {
                        "timestamp": int(now_ts),
                        "today_kW":now_avg_power/1000
                    }
                ]
            })
        
        
        else:
            today_power_kW[0]['data'].append(
                {
                    "timestamp": int(now_ts),
                    "today_kW":now_avg_power/1000
                }
            )
            today_power_kW[0]['timestamp'] = int(now_ts)
            today_power_kW[0]["datetime"] = str(now_element)
            today_power_kW[0]["gatewayid"] =  "lidombmmonitor"
            container.upsert_item(today_power_kW[0])
        logging.info(f'now power {now_avg_power}')
