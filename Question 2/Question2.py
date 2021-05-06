###################################### Simple_Pipeling #############################################
###################################### Ahmad Murad #################################################
###################################### PSUT - Data Engineering  ####################################
###################################### Assignment 1 - Question 2 ###################################
####################################################################################################


from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import json
import csv    
import psycopg2
from psycopg2 import Error
import subprocess
try:
    from faker import Faker
except:
    subprocess.check_call('pip '+ ' install ' +' faker ')
    from faker import Faker

try:
    from pymongo import MongoClient
except:
    subprocess.check_call('pip '+ ' install ' +' pymongo ')    
    from pymongo import MongoClient
###########################################################################################################################################################################

###########################################Generating Dummy Data###########################################################################################################
def generate_dummy_data():    
    output=open('/opt/airflow/logs/data.CSV','w')
    fake=Faker()
    #Faker.seed(2020)
    header=['name','age','country','city','zipcode','state']
    mywriter=csv.writer(output)
    mywriter.writerow(header)
    for r in range(1000):
        mywriter.writerow([fake.name(),fake.random_int(min=18,max=80, step=1), fake.country(), fake.city(),fake.zipcode(),fake.state()])
    output.close()
    print("generate dummy data")

###################################### Creating the table and loading the data into Postgres################################################################################

def load_into_postgres():


    try:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.4",
                                  port = "5432",
                                  database = "postgres")
    except:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.6",
                                  port = "5432",
                                  database = "postgres")
    cursor = connection.cursor()


    create_table_query = '''CREATE TABLE IF NOT EXISTS data_set
    (name  TEXT NOT NULL ,
    age  TEXT NOT NULL ,
    country  TEXT NOT NULL ,
    city TEXT NOT NULL ,
    zipcode TEXT NOT NULL ,
    state TEXT NOT NULL );'''

    cursor.execute(create_table_query)
    connection.commit()
    f = open(r'/opt/airflow/logs/data.CSV', 'r')
    print(f)
    print(cursor.copy_from(f, 'data_set', sep=','))
    connection.commit()
    f.close()
    connection.close()
    print("load the csv file into postgres database")

###################################### Extracting Data - Canada #########################################################################################################

def extract_from_postgres_canada():
    try:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.4",
                                  port = "5432",
                                  database = "postgres")
    except:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.6",
                                  port = "5432",
                                  database = "postgres")
    cursor = connection.cursor()
    sql = "COPY (SELECT * FROM data_set WHERE country = 'Canada') TO STDOUT WITH CSV HEADER "
    try:
        with open("/opt/airflow/logs/canada.csv", "w") as file:
          cursor.copy_expert(sql, file)
    except psycopg2.Error as e:
        print(e)
    connection.commit()
    cursor.close()
    connection.close()
    #print("extract the data and save it as csv")

###################################### Extracting Data - Brazil #####################################################################################################

def extract_from_postgres_brazil():
    try:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.4",
                                  port = "5432",
                                  database = "postgres")
    except:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.6",
                                  port = "5432",
                                  database = "postgres")
    cursor = connection.cursor()
    sql = "COPY (SELECT * FROM data_set WHERE country = 'Brazil') TO STDOUT WITH CSV HEADER "
    try:
        with open("/opt/airflow/logs/brazil.csv", "w") as file:
          cursor.copy_expert(sql, file)
    except psycopg2.Error as e:
        print(e)
    connection.commit()
    cursor.close()
    connection.close()
    #print("extract the data and save it as csv")

###################################### Extracting Data - Ghana #######################################################################################################

def extract_from_postgres_ghana():
    try:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.4",
                                  port = "5432",
                                  database = "postgres")
    except:
        connection = psycopg2.connect(user = "airflow",
                                  password = "airflow",
                                  host = "172.18.0.6",
                                  port = "5432",
                                  database = "postgres")
    cursor = connection.cursor()
    sql = "COPY (SELECT * FROM data_set WHERE country = 'Ghana') TO STDOUT WITH CSV HEADER "
    try:
        with open("/opt/airflow/logs/ghana.csv", "w") as file:
          cursor.copy_expert(sql, file)
    except psycopg2.Error as e:
        print(e)
    connection.commit()
    cursor.close()
    connection.close()
    #print("extract the data and save it as csv")

###################################### Transforming all the CSV files into Json #########################################################################################

def csv_to_json():
    df = pd.read_csv("/opt/airflow/logs/canada.csv")
    df.to_json("/opt/airflow/logs/canada.json")
    print("transform the csv data to json for canada")
    df = pd.read_csv("/opt/airflow/logs/brazil.csv")
    df.to_json("/opt/airflow/logs/brazil.json")
    print("transform the csv data to json for brazil")
    df = pd.read_csv("/opt/airflow/logs/ghana.csv")
    df.to_json("/opt/airflow/logs/ghana.json")
    print("transform the csv data to json for ghana")

###################################### Preparing the Mongo Database called Simple_pipeline  ###############################################################################

def prepare_mongodb_db():
    client = MongoClient('mongo:27017',username='root', password='example')
    dblist = client.list_database_names()
    if "simple_pipline" in dblist:
        print("simple_pipline database exists")
        db = client['simple_pipeline']
    else:
        print("simple_pipline database is created now")
        db = client['simple_pipeline']

###################################### Dumping Data - Canada.json into Canada Collection ###################################################################################

def load_json_into_mongo_canada():
    client = MongoClient('mongo:27017',username='root', password='example')
    db = client['simple_pipeline']
    collection_canada = db['Canada']
    
    with open('/opt/airflow/logs/canada.json') as f:
        file_data = json.load(f)
 
    collection_canada.insert_one(file_data)
    client.close()
    print("load the canada json file into the mongoDB")

###################################### Dumping Data - Brazil.json into Brazil Collection ###################################################################################

def load_json_into_mongo_brazil():
    client = MongoClient('mongo:27017',username='root', password='example')
    db = client['simple_pipeline']
    collection_canada = db['Brazil']
    
    with open('/opt/airflow/logs/brazil.json') as f:
        file_data = json.load(f)
 
    collection_canada.insert_one(file_data)
    client.close()
    print("load the brazil json file into the mongoDB")

###################################### Dumping Data - Ghana.json into Ghana Collection #######################################################################################

def load_json_into_mongo_ghana():
    client = MongoClient('mongo:27017',username='root', password='example')
    db = client['simple_pipeline']
    collection_canada = db['Ghana']
    
    with open('/opt/airflow/logs/ghana.json') as f:
        file_data = json.load(f)
 
    collection_canada.insert_one(file_data)
    client.close()
    print("load the ghana json file into the mongoDB")

###################################### Defining the DAG ###################################################################################################################

pipline_dag = DAG(
    dag_id = 'Simple_Pipline',
    #schedule_interval = "0 0 * * *",
    start_date=datetime(2021,4,5)
)

###################################### Defining the PythonOperators #######################################################################################################

data_preparation_1 = PythonOperator(task_id='prepare_dummy_data',
                             python_callable=generate_dummy_data,
                             dag=pipline_dag)
                             
data_preparation_2 = PythonOperator(task_id='dump_into_postgres',
                             python_callable=load_into_postgres,
                             dag=pipline_dag)

data_extraction_canada = PythonOperator(task_id='extract_from_postgres_canada',
                             python_callable=extract_from_postgres_canada,
                             dag=pipline_dag)
data_extraction_brazil = PythonOperator(task_id='extract_from_postgres_brazil',
                             python_callable=extract_from_postgres_brazil,
                             dag=pipline_dag)
data_extraction_ghana = PythonOperator(task_id='extract_from_postgres_ghana',
                             python_callable=extract_from_postgres_ghana,
                             dag=pipline_dag)
data_transformation = PythonOperator(task_id='transform_data_structure',
                             python_callable=csv_to_json,
                             dag=pipline_dag)
prepare_warehouse = PythonOperator(task_id='prepare_warehouse',
                             python_callable=prepare_mongodb_db,
                             dag=pipline_dag)
data_load_canada = PythonOperator(task_id='load_data_canada',
                             python_callable=load_json_into_mongo_canada,
                             dag=pipline_dag)
data_load_brazil = PythonOperator(task_id='load_data_brazil',
                             python_callable=load_json_into_mongo_brazil,
                             dag=pipline_dag)
data_load_ghana = PythonOperator(task_id='load_data_ghana',
                             python_callable=load_json_into_mongo_ghana,
                             dag=pipline_dag)

###################################### Designing the flow ###############################################################################################################

data_preparation_1 >> data_preparation_2 >> data_extraction_canada >> data_transformation >> prepare_warehouse
data_preparation_2 >> data_extraction_brazil >> data_transformation >> prepare_warehouse
data_preparation_2 >> data_extraction_ghana >> data_transformation >> prepare_warehouse
prepare_warehouse >> data_load_canada
prepare_warehouse >> data_load_brazil
prepare_warehouse >> data_load_ghana

###################################### The End #######################################################################################################################
