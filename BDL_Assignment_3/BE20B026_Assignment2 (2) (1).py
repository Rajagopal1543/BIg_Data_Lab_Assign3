#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import random
import requests
from bs4 import BeautifulSoup
import os
import zipfile


BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
YEAR = '2002' #Can Change the year
DOWNLOAD_DIR = ""
NUM_FILES_REQUIRED = 10 # Can change the number of files 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG('noaa_data_pipeline', default_args=default_args, schedule_interval='@yearly')


fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=f'curl -s {BASE_URL}{YEAR}/ > {DOWNLOAD_DIR}index_{YEAR}.html',
    dag=dag,
)

def parse_and_select_files(**kwargs):
    # Parse the saved webpage
    with open(f'{DOWNLOAD_DIR}index_{YEAR}.html', 'r') as file:
        soup = BeautifulSoup(file, 'html.parser')
    
    # Extract file links
    links = [link.get('href') for link in soup.find_all('a')]
    data_file_links = [link for link in links if link.endswith('.csv')]
    
    # Randomly select the required number of data file links
    selected_files = random.sample(data_file_links, NUM_FILES_REQUIRED)
    # Push selected files to XCom for the next task to use
    kwargs['ti'].xcom_push(key='selected_files', value=selected_files)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=parse_and_select_files,
    provide_context=True,
    dag=dag,
)

def download_files(**kwargs):
    # Retrieve selected files
    ti = kwargs['ti']
    selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
    
    # Download selected file
    for file_url in selected_files:
        response = requests.get(file_url)
        filename = file_url.split('/')[-1]
        with open(f'{DOWNLOAD_DIR}{filename}', 'wb') as file:
            file.write(response.content)

download_files_task = PythonOperator(
    task_id='download_files',
    python_callable=download_files,
    provide_context=True,
    dag=dag,
)

def zip_files(**kwargs):
    # Retrieve selected files
    ti = kwargs['ti']
    selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
    
    with zipfile.ZipFile(f'{DOWNLOAD_DIR}data_{YEAR}.zip', 'w') as zipf:
        for filename in selected_files:
            zipf.write(f'{DOWNLOAD_DIR}{filename}', arcname=filename)
            # We are zipping

zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    provide_context=True,
    dag=dag,
)

move_archive_task = BashOperator(
    task_id='move_archive',
    bash_command=f'mv {DOWNLOAD_DIR}data_{YEAR}.zip /newloc/',
    dag=dag,
)


fetch_page_task >> select_files_task >> download_files_task >> zip_files_task >> move_archive_task


# In[ ]:


from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import geopandas as gpd
import matplotlib.pyplot as plt

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 10),
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
}

dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='A pipeline that processes CSV data with Apache Beam and creates visualizations with GeoPandas',
    schedule_interval='*/1 * * * *',  # Every minute
    catchup=False
)
wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    filepath=f'/newloc/data_{YEAR}.zip',
    timeout=5, #ourtimeout
    poke_interval=1,
    mode='poke',
    dag=dag,
)


unzip_archive = BashOperator(
    task_id='unzip_archive',
    bash_command=f'unzip -o /newloc/data_{YEAR}.zip -d /unzipped/',
    trigger_rule='all_success'
    dag=dag,
)



def process_csv_files():
    
    options = PipelineOptions(
        runner='DirectRunner',
        temp_location='/temp/',
        job_name='process-csv-files'
    )

    
    with beam.Pipeline(options=options) as p:
        def parse_csv(line):
            # The fields were are interested is HourlyWindspeed and HourlyDryBulbTemperature
            parts = line.split(',')
            return {'Lat': parts[2], 'Lon': parts[3], 'HourlyWindSpeed': float(parts[23]), 'HourlyDryBulbTemperature': float(parts[10])}
        #filtering
        def filter_records(record):
            return record['HourlyWindSpeed'] > 3 and record['HourlyDryBulbTemperature'] > -10
        #returning the desired output
        def to_lat_lon_key(record):
            return ((record['Lat'], record['Lon']), [record['HourlyWindSpeed'], record['HourlyDryBulbTemperature']])

        (p 
         | 'Read' >> ReadFromText('/unzipped/*.csv', skip_header_lines=1)
         | 'Parse' >> beam.Map(parse_csv)
         | 'Filter' >> beam.Filter(filter_records)
         | 'ToLatLonKey' >> beam.Map(to_lat_lon_key)
         | 'GroupByKey' >> beam.GroupByKey()
         | 'Write' >> WriteToText('/process/output')
        )

process_task = PythonOperator(
    task_id='process_csv',
    python_callable=process_csv_files,
    dag=dag
)
def compute_monthly_averages():
    options = PipelineOptions(
        runner='DirectRunner',
        temp_location='/temp/',
        job_name='compute-monthly-averages'
    )

    with beam.Pipeline(options=options) as p:
        def parse_csv(line):
            parts = line.split(',')
             def parse_csv(line):
    
                parts = line.split(',')
                return {'Date':parts[1],'Lat': parts[2], 'Lon': parts[3], 'HourlyWindSpeed': float(parts[23]), 'HourlyDryBulbTemperature': float(parts[10])}
       
        def to_monthly_key(record):
            date, lat, lon, dry_bulb_temp, wind_temp = (
                record['Date'], record['Lat'], record['Lon'],
                record['HourlyDryBulbTemperature'], record['HourlyWindTemperature']
            )
            month = date.split('-')[1]
            return ((lat, lon, month), (dry_bulb_temp, wind_temp))

        def compute_monthly_averages(kv_pair):
            key, values = kv_pair
            dry_bulb_temps = [value[0] for value in values]
            wind_temps = [value[1] for value in values]
            avg_dry_bulb_temp = sum(dry_bulb_temps) / len(dry_bulb_temps)
            avg_wind_temp = sum(wind_temps) / len(wind_temps)
            return (key, (avg_dry_bulb_temp, avg_wind_temp))

        (p 
         | 'Read' >> beam.io.ReadFromText('/unzipped/*.csv', skip_header_lines=1)
         | 'Parse' >> beam.Map(parse_csv)
         | 'ToMonthlyKey' >> beam.Map(to_monthly_key)
         | 'GroupByKey' >> beam.GroupByKey()
         | 'ComputeMonthlyAverages' >> beam.Map(compute_monthly_averages)
         | 'WriteMonthlyAverages' >> beam.io.WriteToText('/monthly/output')
        )

compute_monthly_averages_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_averages,
    dag=dag
)

def create_visualizations():
    data=pd.read_csv('monthly/averages/output-*')
    geometry = [Point(lon, lat) for lon, lat in zip(data['lon'], data['lat'])]
    gdf = gpd.GeoDataFrame(data, geometry=geometry)

    fig, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(column='Average', ax=ax, legend=True, cmap='viridis')
    plt.title('Monthly Average Visualization')
    plt.savefig('/visualization/monthly_average.png')

create_visualizations_task = PythonOperator(
    task_id='create_visualizations',
    python_callable=create_visualizations,
    dag=dag
)

cleanup_csv = BashOperator(
    task_id='cleanup_csv',
    bash_command='rm -f /process/output/*.csv',
    dag=dag
)


wait_for_archive >> unzip_archive >> process_task >> compute_monthly_averages_task >> create_visualizations_task >> cleanup_csv

