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
        order by c.timestamp DESC OFFSET 0 LIMIT 1"

    res = []
    for item in container.query_items(
            query=query_string,
            enable_cross_partition_query=True):
        res.append(item)
    
    return res

def main(mytimer: func.TimerRequest) -> None:
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
    ebill_rate = 6 #bath/kWh
    monthly_energy_kWh = 0
    today_energy_kWh = 0
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
            month_energy = []
            today_energy = []
            for i in range(len(now_data[0]['meter_data'])):
                if i < 48:
                    today_energy.append(float(now_data[0]['meter_data'][i+48]['value'])-float(start_data[-1]['meter_data'][i+48]['value']))
                    month_energy.append(float(now_data[0]['meter_data'][i+48]['value'])-float(start_month[-1]['meter_data'][i+48]['value']))
                    
                else:
                    break
            monthly_energy_kWh += sum(month_energy)
            today_energy_kWh += sum(today_energy)
    
    monthly_energy_kWh = monthly_energy_kWh/1000
    today_energy_kWh = today_energy_kWh/1000

    #today
    today_energy_consumption_kwh = today_energy_kWh
    energy_per_m2 = today_energy_kWh/12400 # lido area 12400
    co2_emissions = today_energy_kWh*0.23314

    #monthly
    monthly_energy_consumption = monthly_energy_kWh
    monthly_electricity_bill = monthly_energy_consumption*ebill_rate
    ghgs_equivalent_to_metric_tons_co2 = monthly_energy_kWh*0.000709
    ghgs_equivalent_to_trees = ghgs_equivalent_to_metric_tons_co2*0.060
    ghgs_equivalent_to_barrel_of_oil = ghgs_equivalent_to_metric_tons_co2*0.43
    annual_car_use = ghgs_equivalent_to_metric_tons_co2/4.60
    energy_intensity_kwh_per_m2 = monthly_energy_kWh/12400 # lido area 12400

    '''
    total_energy_consumption = now_acc_energy/1000
    energy_per_m2 = total_energy_consumption/14200 #m2
    ghgs_equivalent_to_metric_tons_co2 = total_energy_consumption*0.000709
    ghgs_equivalent_to_barrel_of_oil = ghgs_equivalent_to_metric_tons_co2*0.43
    ghgs_equivalent_to_trees = ghgs_equivalent_to_metric_tons_co2*0.060
    '''
    '''
        "total_energy_consumption/": {#total_energy_consumption
            "value": total_energy_consumption,
            "unit":"kWh"
        },
    '''
    

    overview = {
        #overview page
         "ghgs_equivalent_to_barrel_of_oil/": {#ghgs_equivalent_to_barrel_of_oil
            "value": ghgs_equivalent_to_barrel_of_oil,
            "unit":"barrel"
        },
        "ghgs_equivalent_to_metric_tons_co2/": {#ghgs_equivalent_to_metric_tons_co2
            "value": ghgs_equivalent_to_metric_tons_co2,
            "unit":"metric ton"
        },
        "ghgs_equivalent_to_trees/": {#ghgs_equivalent_to_trees
            "value": ghgs_equivalent_to_trees,
            "unit":"trees"
        },
         "monthly_electricity_bill/": {#monthly_electricity_bill
            "value": monthly_electricity_bill,
            "unit":"bath"
        },
        "monthly_energy_consumption/": {#monthly_energy_consumption
            "value": monthly_energy_consumption,
            "unit":"kWh"
        },
        "building_rank_table/": {#building_rank_table
            "building_name": "LIDO Connect",
            "energy_intensity_kwh_per_m2": energy_intensity_kwh_per_m2,
            "monthly_energy_consumption_kwh": monthly_energy_consumption,
            "today_energy_consumption_kwh": today_energy_consumption_kwh,
        }
    }
    db.child("building").child("lido").child("pages").child("overview").update(
        overview
    )
    
    dashboard = {    #dashboard
        "energy_per_m2/":{ #energy_per_m2
            "value": energy_per_m2,
            "unit":"kWh/m2"
        },

        #dashboard/co2_emission_equivalent
        "co2_emission_equivalent/annual_car_use/":{#annual_car_use
            "value": annual_car_use,
            "unit":"cars"
        },
        "co2_emission_equivalent/co2_emissions/":{#co2_emissions
            "value": co2_emissions,
            "unit":"kg"
        },
        "co2_emission_equivalent/trees_to_absorb_co2/":{#trees_to_absorb_co2
            "value": ghgs_equivalent_to_trees,
            "unit":"trees"
        }
    }
    #db.update(data)
    
    db.child("building").child("lido").child("pages").child("dashboard").update(
        dashboard
    )

    o = db.child("building").child("lido").child("pages").child("overview").get().val()
    d = db.child("building").child("lido").child("pages").child("dashboard").get().val()

    '''
    ret = {
        'firebase':o,
        'cosmos':d
    }
    return func.HttpResponse(f"ok")
    '''