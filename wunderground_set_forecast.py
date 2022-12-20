# -*- coding: utf-8 -*-
"""
Created on Wed Nov  9 11:56:54 2022

"""
import pandas as pd
import requests
import numpy as np
from datetime import timedelta
import json
import datetime
from dateutil import tz
def get_token():
    username = "your-username.com"
    password = "your-pasword"
    base_url = "https://your-api/api"          
    try:
        headers = {}
        params = {'username': username, 'password': password}
        api_token = base_url + "/get-token"
        r = requests.post(url=api_token, headers=headers, params=params)
        return r.json()
    except Exception as e:
        print("Error in getting token : {a}".format(a=e))
def set_Forecast(client_id,data,source,plant_id):
        try:
            headers = {'token':get_token()['access_token'],'Content-Type': 'application/json'}
            params = {"client_id":str(client_id),"source":"wunderground_forecast","data":data}
            api_url = "https://your-api/api/weather/setForecast"
            r = requests.post(url=api_url, data=json.dumps(params), headers=headers)
            return r.json()
        except Exception as e:
            print(e)
def convert_tz_pd(df_col, from_zone, to_zone):
    from_zone = tz.gettz(from_zone)
    to_zone = tz.gettz(to_zone)

    df_col = df_col.apply(lambda x: x.replace(tzinfo=from_zone))
    df_col = df_col.apply(lambda x: x.astimezone(to_zone))
    df_col = df_col.apply(lambda x: x.tz_localize(None))
    return df_col
client_id = 110#Renew
plant_id =1#[1,19]
latitude = ['31.49666864']
longitude = ['78.18906766']
for i in range(len(latitude)):
    api_k = 'api-key'
    url = 'https://wunderground-api-url?latitude='+latitude[i]+'&longitude='+longitude[i]+'&hourly=snowfall,temperature_2m,precipitation,relativehumidity_2m,dewpoint_2m,apparent_temperature,pressure_msl,cloudcover,windspeed_10m,winddirection_10m,windgusts_10m,visibility&timezone=auto'
    res = requests.get(url)
    d = res.json()  
    d1 = pd.DataFrame(d)
    df = pd.DataFrame()
    for j in range(0,13):
        df[d1.index[j]] = d1['hourly'][j]
    df['datetime'] = pd.to_datetime(df['time'])
    df['datetime_utc'] =df['datetime'] - timedelta(hours=5,minutes=30)
    df['datetime_local']=df['datetime'] 
    df['datetime_local']=df['datetime_local'].dt.strftime('%Y:%m:%d %H:%M:%S') 
    df['datetime_utc']=df['datetime_utc'].dt.strftime('%Y:%m:%d %H:%M:%S')
    df['temperature'] = df['temperature_2m']
    df['temperatureDewPoint'] = df['dewpoint_2m']
    df['temperatureWindChill'] = None
    df['pressureMeanSeaLevel'] = df['pressure_msl']#*33.86 
    df['precip_probability']=None
    df['heat_index']=None
    df['precip_intensity']=df['precipitation']
    df['cloudCover']=df['cloudcover']/100
    df.datetime_local = df.datetime_local.astype('str')
    df.datetime_utc = df.datetime_utc.astype('str')
    df.rename(columns={'cloudCover':'cloud_cover',
               'temperatureDewPoint':'dew_point',
               'winddirection_10m':'wind_bearing',
               'windspeed_10m':'wind_speed',
               'temperatureWindChill': 'wind_chill',
               'windgusts_10m': 'wind_gust',
               'pressureMeanSeaLevel':'pressure',
               'precipChance':'pop',
               'precipType':'precip_type',
               'uvIndex':'uv_index',
               'qpfSnow':'snow',
               'iconCode':'icon'}, inplace=True)

    df['plant_id'] =  plant_id
    df['fctcode']  = None
    df['sunrise'] = None
    df['sunset'] = None
    df['summary'] = None    
    df['ozone'] = None
    df['precip_accumulation'] = None
    df['icon'] = None
    df['humidity'] = df['relativehumidity_2m']/100
    df['precip_type'] = None
    df['qpf'] = None
    df['uv_index'] = None
    df['snow'] = df['snowfall']
    df['pop'] = None
    df.reset_index(drop=True, inplace=True)
    cols = ['plant_id','datetime_utc','datetime_local', 'cloud_cover', 'apparent_temperature', 'temperature', 'humidity',
            'dew_point', 'wind_bearing', 'wind_speed', 'wind_chill','wind_gust','heat_index', 'pressure',
            'qpf', 'uv_index', 'snow', 'pop', 'fctcode','ozone', 'precip_accumulation', 'precip_intensity',
            'precip_probability','precip_type', 'visibility', 'sunrise', 'sunset','icon', 'summary']    
    df = df[cols]
    b=df
    data = b.to_json(orient='records')
    result = set_Forecast(client_id,data,'wunderground_forecast',plant_id)
    print(result)
