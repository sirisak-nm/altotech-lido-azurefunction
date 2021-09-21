import logging

import azure.functions as func
from azure.cosmos import CosmosClient
from datetime import datetime, timedelta
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

def acc_energy_ts(**kw):
    device_id = ["mbm1","mbm2","mbm3","mbm4"]
    mbm = []
    for item in device_id:
            temp = query_cosmos_ts(
                device_id=item,
                start_time=kw['start_ts'], 
                end_time=kw['start_ts']+600 
            )
            meter_data_start = []
            for i in range(len(temp[0]['meter_data'])):
                if i > 47:
                    meter_data_start.append(float(temp[0]['meter_data'][i]['value']))
            
            temp = query_cosmos_ts(
                device_id=item,
                start_time=kw['end_ts']-600, 
                end_time=kw['end_ts'] 
            )
            meter_data_end = []
            for i in range(len(temp[-1]['meter_data'])):
                if i > 47:
                    meter_data_end.append(float(temp[-1]['meter_data'][i]['value']))
            
            mbm.append(
                sum(meter_data_end)-sum(meter_data_start)
            )
    return sum(mbm)

def avg_power_ts(**kw):
    device_id = ["mbm1","mbm2","mbm3","mbm4"]
    mbm = []
    for item in device_id:
        query_string=f"SELECT * from c where\
        c.device_id = '{item}' \
        and c.timestamp >= {kw['start_ts']}\
        and c.timestamp <= {kw['end_ts']} \
        order by c.timestamp DESC OFFSET 0 LIMIT 1440"
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


def main(mytimer: func.TimerRequest) -> None:
    logging.info('start')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    # now
    tz = timezone('Asia/Bangkok')
    utc_now = datetime.utcnow()
    now_element = pytz.utc.localize(utc_now, is_dst=None).astimezone(tz)
    now_ts = datetime.timestamp(now_element)
    logging.info(f"utc now: {utc_now}")
    logging.info(f"now timezone {tz}: {now_element}")
    logging.info(f"now timestamp timezone {tz}: {now_ts}")
    
    
    # start_day
    today = now_element.strftime("%Y-%m-%d 00:00")
    today_element = datetime.strptime(today, "%Y-%m-%d %H:%M")
    start_ts = datetime.timestamp(today_element)
    start_ts = int(start_ts) - (60*60*7)
    logging.info(f"today: {today}")
    logging.info(f"today timezone {tz}: {today_element}")
    logging.info(f"today timestamp timezone {tz}: {start_ts}")

    # start_time
    today = now_element.strftime("%Y-%m-%d 23:59")
    today_element = datetime.strptime(today, "%Y-%m-%d %H:%M")
    today_element = pytz.utc.localize(today_element, is_dst=None).astimezone(tz)
    end_ts = start_ts + (60*60*24) - 60

    # daily_energy_kWh
    daily_energy_kWh = []
    for item in container.query_items(
        query=f"SELECT * FROM c WHERE c.device_id = 'building_daily_energy_info' order by c.timestamp DESC OFFSET 0 LIMIT 1",
        enable_cross_partition_query=True):
        daily_energy_kWh.append(item)
    logging.info(f'{daily_energy_kWh}')
    
    if not daily_energy_kWh:
        start_yesterday = datetime.fromtimestamp(start_ts)
        start_yesterday -= timedelta(days=1)
        start_yesterday_ts = datetime.timestamp(start_yesterday)

        end_yesterday = datetime.fromtimestamp(end_ts)
        end_yesterday -= timedelta(days=1)
        end_yesterday_ts = datetime.timestamp(end_yesterday)
        
        yesterday_acc_energy = acc_energy_ts(start_ts=start_yesterday_ts,end_ts=end_yesterday_ts)
        yesterday_avg_power = avg_power_ts(start_ts=start_yesterday_ts,end_ts=end_yesterday_ts)
        now_acc_energy = acc_energy_ts(start_ts=start_ts,end_ts=now_ts)
        now_avg_power = avg_power_ts(start_ts=start_ts,end_ts=now_ts)
        logging.info(f'yerterday energy: {yesterday_acc_energy}')
        logging.info(f'yerterday power: {yesterday_avg_power}')
        logging.info(f'now energy: {now_acc_energy}')
        logging.info(f'now power {now_avg_power}')
        start_yesterday += timedelta(days=1)
        container.upsert_item({
            "device_id": "building_daily_energy_info",
            "timestamp": int(now_ts),
            "datetime":str(now_element),
            "gatewayid": "lidombmmonitor",
            'today_timestamp': int(start_ts),
            "today_datetime": today,
            "trigger" :"5minute",
            'data': [
                {
                    "timestamp": int(start_yesterday_ts),
                    "datetime": start_yesterday.strftime("%Y-%m-%d"),
                    "daily_kW": yesterday_avg_power/1000, 
                    "daily_kWh": yesterday_acc_energy/1000
                },
                {
                    "timestamp": int(start_yesterday_ts),
                    "datetime": now_element.strftime("%Y-%m-%d"),
                    "daily_kW": now_avg_power/1000,
                    "daily_kWh": now_acc_energy/1000
                }
            ]
        })
    
    else:
        now_acc_energy = acc_energy_ts(start_ts=start_ts,end_ts=now_ts)
        now_avg_power = avg_power_ts(start_ts=start_ts,end_ts=now_ts)
    
        if int(daily_energy_kWh[0]['today_timestamp']) != int(start_ts):
            start_yesterday = datetime.fromtimestamp(start_ts)
            start_yesterday -= timedelta(days=1)
            start_yesterday_ts = datetime.timestamp(start_yesterday)

            end_yesterday = datetime.fromtimestamp(end_ts)
            end_yesterday -= timedelta(days=1)
            end_yesterday_ts = datetime.timestamp(end_yesterday)
        
            yesterday_acc_energy = acc_energy_ts(start_ts=start_yesterday_ts,end_ts=end_yesterday_ts)
            yesterday_avg_power = avg_power_ts(start_ts=start_yesterday_ts,end_ts=end_yesterday_ts)
            logging.info(f'yerterday energy: {yesterday_acc_energy}')
            logging.info(f'yerterday power: {yesterday_avg_power}')
            logging.info(f'now energy: {now_acc_energy}')
            logging.info(f'now power {now_avg_power}')
            start_yesterday += timedelta(days=1)
        
            daily_energy_kWh[0]['data'][-1] = {
                "timestamp": int(start_yesterday_ts),
                "datetime": start_yesterday.strftime("%Y-%m-%d"),
                "daily_kWh": yesterday_acc_energy/1000,
                "daily_kW": yesterday_avg_power/1000
            }
            daily_energy_kWh[0]['data'].append({
                "timestamp": int(now_ts),
                "datetime": now_element.strftime("%Y-%m-%d"),
                "daily_kWh": now_acc_energy/1000,
                "daily_kW": now_avg_power/1000
            })
            daily_energy_kWh[0]['timestamp'] = int(now_ts)
            daily_energy_kWh[0]["datetime"] = str(now_element)
            daily_energy_kWh[0]["gatewayid"] =  "lidombmmonitor"
            daily_energy_kWh[0]["device_id"] = "building_daily_energy_info"
            daily_energy_kWh[0]["trigger"] = "5minute"
            daily_energy_kWh[0]['today_timestamp']= int(start_ts)
            daily_energy_kWh[0]["today_datetime"]= today
            container.upsert_item(daily_energy_kWh[0])
        
        else:
            logging.info(f'now energy: {now_acc_energy}')
            logging.info(f'now power {now_avg_power}')
            daily_energy_kWh[0]['data'][-1] = {
                "timestamp": int(now_ts),
                "datetime": now_element.strftime("%Y-%m-%d"),
                "daily_kWh": now_acc_energy/1000,
                "daily_kW": now_avg_power/1000
            }
            daily_energy_kWh[0]["datetime"] = str(now_element)
            daily_energy_kWh[0]['timestamp'] = int(now_ts)
            daily_energy_kWh[0]['date'] = int(start_ts)
            daily_energy_kWh[0]["gatewayid"] =  "lidombmmonitor"
            daily_energy_kWh[0]["device_id"] = "building_daily_energy_info"
            daily_energy_kWh[0]["trigger"] = "5minute"
            container.upsert_item(daily_energy_kWh[0])
