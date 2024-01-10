from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def kelvin_to_celcius(temp_in_celcius):
    temp_in_celcius = (temp_in_celcius - 273.15)
    return temp_in_celcius


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celcius = kelvin_to_celcius(data["main"]["temp"])
    feels_like_celcius= kelvin_to_celcius(data["main"]["feels_like"])
    min_temp_celcius = kelvin_to_celcius(data["main"]["temp_min"])
    max_temp_celcius = kelvin_to_celcius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp_celcius,
                        "Feels Like (C)": feels_like_celcius,
                        "Minimun Temp (C)":min_temp_celcius,
                        "Maximum Temp (C)": max_temp_celcius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "ASIAZ5W6WL2DJMC4UUXL", "secret": "OFEWu9k/Eh6FWVpgDXS+BfHe2Gzme0HfyMoaYrD1", "token": "FwoGZXIvYXdzEIH//////////wEaDJZvznjz4QVUs8zdEiJqUAUB0xD85MtqOEoNkIW6ZAWoshOam6jR+tROsHqA4saSf1QT4GyzXlV7W5Mp1JSixCXUz8F/ex7FqnFM3PzY/Esyl3Tgob3fFbBhJnd8fJ9U3oHy8U841vfWlMIsHP5rj4AdzzjXQOmypCi55PqsBjIoy4XS/A9UX4VBmL4KyBR3j0U4vXRtc9U6HKYFgutZeG7EjIO5bKuOug=="}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_Bangkok_' + dt_string
    df_data.to_csv(f"s3://weather-s3-lake-bucket/{dt_string}.csv", index=False, storage_options=aws_credentials)





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 10),
    'email': ['viritpol.ss@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:



        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready1',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Bangkok&appid=b481837e915e0b60009d27b00f833192'
        )



        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Bangkok&appid=b481837e915e0b60009d27b00f833192',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )



        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )


        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data

