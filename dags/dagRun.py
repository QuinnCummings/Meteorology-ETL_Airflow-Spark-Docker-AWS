import json
import pandas as pd
import os
import requests
from io import StringIO
from io import BytesIO
# import boto3
import boto3
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2
import sys
sys.path.insert(0, '/opt/airflow/sparkFiles')
from sparkProcess import run_spark

def call_api(ti):
    '''Collects data from time7 api
    Args: bucket?
    Returns: None
    '''
    url = 'https://www.7timer.info/bin/meteo.php?lon=-73.971&lat=40.776&ac=0&unit=metric&output=json&tzshift=0'
    req = requests.get(url)
    j = req.json()

    init_time = []
    timepoint = []
    temperature = []
    prec_type = []
    prec_amount = []
    snow_depth = []
    cloud_cover = []
    wind_direction = []
    wind_speed = []

    for dataseries in j['dataseries']:
        timepoint.append(dataseries['timepoint'])
        temperature.append(dataseries['temp2m'])
        prec_type.append(dataseries['prec_type'])
        prec_amount.append(dataseries['prec_amount'])
        snow_depth.append(dataseries['snow_depth'])
        cloud_cover.append(dataseries['cloudcover'])
        wind_direction.append(dataseries['wind10m']['direction'])
        wind_speed.append(dataseries['wind10m']['speed'])
    init_time = [j['init']] * len(timepoint)
    
    df_structure = {
        'init' : init_time,
        'timepoint' : timepoint,
        'temperature' : temperature,
        'prec_type' : prec_type,
        'prec_amount' : prec_amount,
        'snow_depth' : snow_depth,
        'cloud_cover' : cloud_cover,
        'wind_direction' : wind_direction,
        'wind_speed' : wind_speed
    }
    df = pd.DataFrame(df_structure)
    df.to_csv(r'/opt/airflow/sparkFiles/parsedData.csv')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    
    hook = S3Hook()
    filename = 'meteo_data_{}.csv'.format(j['init'])
    hook.load_string (string_data = csv_buffer.getvalue(),
                    key = filename,
                    bucket_name = 'meteo-data',
                    replace = True
                    )
    
    ti.xcom_push(key='filename', value=filename)
   
   
def load_to_redshift(ti):
    
    dbname = ''
    host =  ''
    port = ''
    user = ''
    password = ''
    awsIAMrole = ''

    
    conn = psycopg2.connect(dbname= dbname, host=host, port= port, user= user, password= password)
    cursor = conn.cursor()
    
    sql = f"""sql"""
                  
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    
defaultArgs = {
    'owner': 'Quinn_Cummings',
    'start_date': datetime.datetime(2021, 1, 1),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30)
}

with DAG('analyze_json_data',
         schedule_interval = '@daily',
         default_args = defaultArgs,
         catchup = False) as dag:
    
    getData = PythonOperator(
        task_id = 'getData',
        python_callable = call_api
    )

    processData = PythonOperator(
        task_id='processData',
        python_callable = run_spark
    )
    
    loadRedshift = PythonOperator(
        task_id = 'loadRedshift',
        python_callable = load_to_redshift
    )
    getData >> processData >> loadRedshift