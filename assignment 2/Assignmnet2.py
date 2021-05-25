###################################### Simple_Pipeling #############################################
###################################### Ahmad Murad #################################################
###################################### PSUT - Data Engineering  ####################################
###################################### Assignment 2 - Question 1 ###################################
####################################################################################################


from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import matplotlib 
import matplotlib.pyplot as plt
import sklearn
import json
import csv    
import psycopg2
from psycopg2 import Error
import subprocess
import time 
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

DF_all =[] 
DF_JO_new_3 = pd.DataFrame()

###########################################################################################################################################################################

###########################################initialization_data###########################################################################################################
def initialization_data():    
    List_of_days = []
    for year in range(2020,2022):
        for month in range(1,13):
            for day in range(1,32):
                month=int(month)
                if day <=9:
                    day=f'0{day}'

                if month <= 9 :
                    month=f'0{month}'
                List_of_days.append(f'{month}-{day}-{year}')
    return List_of_days

###################################### collection_JO################################################################################
def Get_DF_i(Day):
    DF_i=None
    try: 
        URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day=pd.read_csv(URL_Day)
        DF_day['Day']=Day
        cond=(DF_day.Country_Region=='Jordan')
        Select_columns=['Day','Country_Region', 'Last_Update',
              'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
              'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_i=DF_day[cond][Select_columns].reset_index(drop=True)
    except:
        pass
    return DF_i

def collection_JO():
    Start=time.time()
    for Day in initialization_data():
        DF_all.append(Get_DF_i(Day))
        print(Day)
    DF_JO=pd.concat(DF_all).reset_index(drop=True)
    DF_JO.to_csv('/opt/airflow/logs/Temp.csv')
    End=time.time()
    Time_in_sec=round((End-Start)/60,2)
    print(f'It took {Time_in_sec} minutes to get all data')
###################################### fitting_data #########################################################################################################

def fitting_data():
    
    DF_JO=pd.read_csv('/opt/airflow/logs/Temp.csv')
        # Create DateTime for Last_Update
    DF_JO['Last_Updat']=pd.to_datetime(DF_JO.Last_Update, infer_datetime_format=True)  
    DF_JO['Day']=pd.to_datetime(DF_JO.Day, infer_datetime_format=True)  

    DF_JO['Case_Fatality_Ratio']=DF_JO['Case_Fatality_Ratio'].astype(float)
    DF_JO_new=DF_JO.copy()
    DF_JO_new.index=DF_JO_new.Day
    Select_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_JO_new_2=DF_JO_new[Select_Columns]

    min_max_scaler = MinMaxScaler()

    DF_JO_new_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_JO_new_2[Select_Columns]),columns=Select_Columns)
    DF_JO_new_3.index=DF_JO_new.Day
    DF_JO_new_3['Day']=DF_JO_new.Day
    DF_JO_new_3.to_csv('/opt/airflow/logs/DF_JO_new_3.csv')


###################################### Extracting Data - Brazil #####################################################################################################

def extract_png():
    font = {'weight' : 'bold',
        'size'   : 18}

    matplotlib.rc('font', **font)
    Select_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_JO_new_3=pd.read_csv('/opt/airflow/logs/DF_JO_new_3.csv')
    DF_JO_new_3.index=DF_JO_new_3.Day
    DF_JO_new_3[Select_Columns].plot(figsize=(20,10))
    plt.savefig('/opt/airflow/logs/JO_scoring_report.png')

###################################### Extracting Data - Ghana #######################################################################################################

def extract_csv():
    DF_JO_new_3=pd.read_csv('/opt/airflow/logs/DF_JO_new_3.csv')
    DF_JO_new_3.to_csv('/opt/airflow/logs/JO_scoring_report.csv')

###################################### Transforming all the CSV files into Json #########################################################################################

def extract_to_postgres():
    
    host="postgres" # use "localhost" if you access from outside the localnet docker-compose env 
    database="postgres"
    user="airflow"
    password="airflow"
    port='5432'
    Day = datetime.today()
    Daystr=f'{Day.day}' +'_' + f'{Day.month}' + '_'+ f'{Day.year}'
    DF_JO_new_3=pd.read_csv('/opt/airflow/logs/DF_JO_new_3.csv')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    DF_JO_new_3.to_sql(f'Jordan_scoring_report_{Daystr}', engine,if_exists='replace',index=False)

###################################### Preparing the Mongo Database called Simple_pipeline  ###############################################################################



###################################### Defining the DAG ###################################################################################################################

Covid_pipline_dag = DAG(
    dag_id = 'JO_Covid_Pipline',
    #schedule_interval = "0 0 * * *",
    start_date=datetime(2021,5,25)
)

###################################### Defining the PythonOperators #######################################################################################################

data_collection_JO = PythonOperator(task_id='data_collection_JO',
                             python_callable=collection_JO,
                             dag=Covid_pipline_dag)

data_scoring_JO = PythonOperator(task_id='data_scoring_JO',
                             python_callable=fitting_data,
                             dag=Covid_pipline_dag)
data_extraction_png = PythonOperator(task_id='extracting_png',
                             python_callable=extract_png,
                             dag=Covid_pipline_dag)
data_extraction_csv = PythonOperator(task_id='extract_csv',
                             python_callable=extract_csv,
                             dag=Covid_pipline_dag)
data_load_postgres = PythonOperator(task_id='load_to_postgres',
                             python_callable=extract_to_postgres,
                             dag=Covid_pipline_dag)

###################################### Designing the flow ###############################################################################################################

data_collection_JO >> data_scoring_JO >> data_extraction_png 
data_scoring_JO >> data_extraction_csv
data_scoring_JO >> data_load_postgres

###################################### The End #######################################################################################################################
