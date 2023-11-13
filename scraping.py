from bs4 import BeautifulSoup
import requests as r
import pandas as pd
from google.cloud import storage
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



headers =  {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}

def get_ids(res):
    soup = BeautifulSoup(res.text, 'html.parser')
    tbodys= soup.find_all('tbody')
    trs = tbodys[11].find_all('tr')
    ths = []
    codigos = []
    equipos = []

    for tr in trs:
        ths.append(str(tr.find('th', class_='left')))
    for th in ths:
        texto_split= th.split('href="/es/equipos/')
        codigos.append(texto_split[1].split('/Estadisticas')[0])
        equipos.append(texto_split[1].split('>vs.')[1].split('</a>')[0].strip())
    
    return equipos, codigos

def scraping():
    url = 'https://fbref.com/es/comps/9/Estadisticas-de-Premier-League'
    res = r.get(url,headers=headers)
    equipos, codigos = get_ids(res)

    df=pd.DataFrame({'Codigo':codigos,'Equipo': equipos})
    df.to_csv('/tmp/Equipos.csv')

    #inicia el cliente storage
    storage_client = storage.Client()
    bucket = storage_client.bucket('pruebadags')

    file_name = 'Equipos.csv'
    blob = bucket.blob(file_name)

    blob.upload_from_filename('/tmp/Equipos.csv')

    return file_name

def add_team(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids = 'task_scraping')

    storage_client = storage.Client()
    bucket = storage_client.bucket('pruebadags')
    blob = bucket.blob(file_name)

    destination_file = '/tmp/Equipos.csv'
    blob.download_to_filename(destination_file)

    df = pd.read_csv(destination_file)
    new_team = {'Codigo': '643108','Equipo': 'The_sups_team'}
    new_team = pd.DataFrame([new_team], columns=new_team.keys())

    df = pd.concat([df,new_team], ignore_index=True)
    df.to_csv(destination_file)
    file_name = 'sup_team.csv'
    blob = bucket.blob(file_name)
    blob.upload_from_filename(destination_file)

default_args = {
    'owner': 'Adrian',
    'depends_on_past': True,
    'start_date': datetime.datetime(2023,11,8),
    'retries':1,
    'retry_delay': datetime.timedelta(minutes=.5),
    'project_id': 'vocal-byte-400601'
}

with DAG('Scraping',
         default_args=default_args,
         schedule_interval = '0 0 * * 0') as dag:
    
    t_begin = DummyOperator(task_id='begin')


    task_scraping = PythonOperator(task_id='task_scraping',
                                   python_callable = scraping,
                                   depends_on_past=True,
                                   
                                   dag=dag)
    
    task_add_team = PythonOperator(task_id='task_add_team',
                                    provide_context=True,
                                    python_callable = add_team,
                                    depends_on_past=True,
                                
                                    dag=dag)
    
    t_end = DummyOperator(task_id='end')

t_begin >> task_scraping >> task_add_team >> t_end
    

    








    


