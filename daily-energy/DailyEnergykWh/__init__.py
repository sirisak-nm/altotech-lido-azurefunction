import logging

import azure.functions as func
from azure.cosmos import CosmosClient
from datetime import datetime, timedelta
from pytz import timezone
import pytz
import pyrebase


config = {
    "apiKey": "AIzaSyAyM699YBXl2aZfO5eNmuK4H2UXst5QePI",
    "authDomain": "altolido-ec6c7.firebaseapp.com",
    "databaseURL": "https://altolido-ec6c7-default-rtdb.asia-southeast1.firebasedatabase.app/",
    "storageBucket": "altolido-ec6c7.appspot.com",
}
firebase = pyrebase.initialize_app(config)
db = firebase.database()

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

    # start_time
    today = now_element.strftime("%Y-%m-%d 23:59")
    today_element = datetime.strptime(today, "%Y-%m-%d %H:%M")
    end_ts = datetime.timestamp(today_element)

    # daily_energy_kWh
    daily_energy_kWh = []
    for item in container.query_items(
        query=f"SELECT * FROM c WHERE c.device_id = 'daily_energy_kWh' order by c.timestamp DESC",
        enable_cross_partition_query=True):
        daily_energy_kWh.append(item)
    '''
    daily_energy_kWh = query_cosmos_ts(
        device_id='daily_energy_kWh',
        start_time=now_ts-600, # 10 minutes ago
        end_time=now_ts # now
    )
    '''
    
    if not daily_energy_kWh:
        start_yesterday = datetime.fromtimestamp(start_ts)
        start_yesterday -= timedelta(days=1)
        start_yesterday_ts = datetime.timestamp(start_yesterday)

        end_yesterday = datetime.fromtimestamp(end_ts)
        end_yesterday -= timedelta(days=1)
        end_yesterday_ts = datetime.timestamp(end_yesterday)
        
        yesterday_acc_energy = acc_energy_ts(start_ts=start_yesterday_ts,end_ts=end_yesterday_ts)
        now_acc_energy = acc_energy_ts(start_ts=start_ts,end_ts=now_ts)
        container.upsert_item({
            "device_id": "daily_energy_kWh",
            "timestamp": int(now_ts),
            "date": int(start_ts),
            'data': [
                {
                    "timestamp": int(start_yesterday_ts),
                    "value": yesterday_acc_energy/1000
                },
                {
                    "timestamp": int(start_yesterday_ts),
                    "value": now_acc_energy/1000
                }
            ]
        })
    
    else:
        now_acc_energy = acc_energy_ts(start_ts=start_ts,end_ts=now_ts)
    
        if int(daily_energy_kWh[0]['date']) != int(start_ts):
            start_yesterday = datetime.fromtimestamp(start_ts)
            start_yesterday -= timedelta(days=1)
            start_yesterday_ts = datetime.timestamp(start_yesterday)

            end_yesterday = datetime.fromtimestamp(end_ts)
            end_yesterday -= timedelta(days=1)
            end_yesterday_ts = datetime.timestamp(end_yesterday)
        
            yesterday_acc_energy = acc_energy_ts(start_ts=start_yesterday_ts,end_ts=end_yesterday_ts)
        
            daily_energy_kWh[0]['data'][-1] = {
                        "timestamp": int(start_yesterday_ts),
                        "value": yesterday_acc_energy/1000
                    }
            daily_energy_kWh[0]['data'].append({
                "timestamp": int(now_ts),
                "value": now_acc_energy/1000
            })
            daily_energy_kWh[0]['date'] = int(start_ts)
            daily_energy_kWh[0]['timestamp'] = int(now_ts)
            container.upsert_item(daily_energy_kWh[0])
        
        else:
            daily_energy_kWh[0]['data'][-1] = {
                "timestamp": int(now_ts),
                "value": now_acc_energy/1000
            }
            daily_energy_kWh[0]['timestamp'] = int(now_ts)
            daily_energy_kWh[0]['date'] = int(start_ts)
            container.upsert_item(daily_energy_kWh[0])
        
    total_energy_consumption = now_acc_energy/1000
    energy_per_m2 = total_energy_consumption/14200 #m2
    ghgs_equivalent_to_metric_tons_co2 = total_energy_consumption*0.000709
    ghgs_equivalent_to_barrel_of_oil = ghgs_equivalent_to_metric_tons_co2/0.43
    ghgs_equivalent_to_trees = ghgs_equivalent_to_metric_tons_co2/0.060

    data = {
        "building/lido/pages/overview/ghgs_equivalent_to_trees/": {
            "value": ghgs_equivalent_to_trees,
            "unit":"trees"
        },
        "building/lido/pages/overview/ghgs_equivalent_to_barrel_of_oil/": {
            "value": ghgs_equivalent_to_barrel_of_oil,
            "unit":"barrel"
        },
        "building/lido/pages/overview/total_energy_consumption/": {
            "value": total_energy_consumption,
            "unit":"kWh"
        },
        "building/lido/pages/overview/ghgs_equivalent_to_metric_tons_co2/": {
            "value": ghgs_equivalent_to_metric_tons_co2,
            "unit":"metric ton"
        },
        "building/lido/pages/dashboard/co2_emission_equivalent/co2_emissions":{
            "value": total_energy_consumption*0.23314,
            "unit":"kg"
        },
        "building/lido/pages/dashboard/co2_emission_equivalent/annual_car_use/":{
            "value": ghgs_equivalent_to_metric_tons_co2/4.60,
            "unit":"cars"
        },
        "building/lido/pages/dashboard/co2_emission_equivalent/trees_to_absorb_co2/":{
            "value": ghgs_equivalent_to_trees,
            "unit":"trees"
        },
        "building/lido/pages/dashboard/energy_per_m2":{
            "value": energy_per_m2,
            "unit":"kWh/m2"
        }
    }
    db.update(data)
