from email import header
from genericpath import exists
from operator import index
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import DAG
from airflow.models import Variable
from airflow import AirflowException

### SENSORS
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.http.sensors.http import HttpSensor

### STANDARD PACKAGES
import sys
import os
import shutil
from zipfile import ZipFile
import mysql.connector
from mysql.connector import Error
import json
from random import uniform
from datetime import datetime
from numpy.lib.stride_tricks import DummyArray
import pandas as pd 

sys.path.append('../scripts')

import DropboxAPI_func
import mySQL_db_func
import preprocessing

db_token = Variable.get("Dropbox_API_Token")
mysqlDBName = Variable.get("MySQL_DBname")
mysqlUserName = Variable.get("MySQL_user")
mysqlPassword = Variable.get("MySQL_pw")
mysqlHost = Variable.get("MySQL_host")
dataDirectory = Variable.get("Mounted_dir_path")  
dbxPath = Variable.get("Dropbox_path")
csvdataDirectory = Variable.get("CSV_Data_dir")

Dropbox_inst = DropboxAPI_func.dropbox_func(accestoken=db_token,
                                            dbxPath=dbxPath,
                                            dataPath=dataDirectory)
Processing_inst = preprocessing.main()
MySQL_inst = mySQL_db_func.MySQL_insertion(db_host=mysqlHost,
                                            db_name=mysqlDBName,
                                            user=mysqlUserName,
                                            pw=mysqlPassword)

def get_new_files(ti):
    newFilesFound = False
    newFilesList = list()
    existing_files = MySQL_inst.get_files_names()
    dropbox_files = Dropbox_inst.files_list()

    for item in dropbox_files:
        if item.name not in existing_files:
            print("new file found {}".format(item.name))
            newFilesList.append(item.name)
            newFileFound = True

    if newFilesFound == True:
        ti.xcom_push(key='new_files_list',value=newFilesList)
        return 'download_data'
    else:
        return 'no_new_files'


def download_data(ti):
    os.mkdir(dataDirectory)
    newFilesList = ti.xcom_pull(key='new_files_list')
    Dropbox_inst.downloadFileFromList(newFilesList)


def processing_data(ti):
    os.mkdir(csvdataDirectory)
    newFilesList = ti.xcom_pull(key='new_files_list')
    for file_name in newFilesList: 
        full_file_path = "{}apple_health_export/{}.xml".format(dataDirectory, file_name)
        df = Processing_inst.process_dataframe(full_file_path)
        df.to_csv("{}{}.csv".format(csvdataDirectory, file_name))


def import_to_mysql(ti):
    newFilesList = ti.xcom_pull(key='new_files_list')
    MySQL_inst.push_files_names(newFilesList)
    for file_name in newFilesList:
        full_path = "{}{}.csv".format(csvdataDirectory, file_name)
        MySQL_inst.insertIntoMYSQL(full_path)


def delete_temporary_files():
    try:
        shutil.rmtree(csvdataDirectory)
    except OSError as e:
        print(e)

    try:
        shutil.rmtree(dataDirectory)
    except OSError as e:
        print(e)

def no_new_files():
    print("No new files found")  

def finish_processing(ti):
    try:
        filesToProcess = ti.xcom_pull(key='new_files_list')
        print(len(filesToProcess), " files processed.")       
    except:
        print("")
    
    
## Dag Definition 

with DAG('AppleHealthData_Dag', schedule_interval='@daily', default_args=default_args, catchup=False, tags=['APPLEHEALTH']) as dag:
        is_dropbox_Mysql_available = [HttpSensor(
                                        task_id='is_dropbox_available',
                                        http_conn_id='HTTP_DROPBOX',
                                        endpoint=''),
                                    HttpSensor(
                                        task_id='is_Mysql_available',
                                        http_conn_id='HTTP_MYSQL',
                                        endpoint='')
                                    ]
        check_for_new_files = BranchPythonOperator(
                                task_id='check_for_new_files',
                                python_callable=get_new_files    
                                    )
        download_data = PythonOperator(
                                task_id='download_data',
                                python_callable=download_data
                                    )
        process_data = PythonOperator(
                                task_id='process_data',
                                python_callable=processing_data
                                    )
        detect_import_file = FileSensor(
                                task_id='detect_import_file',
                                poke_interval=30,
                                filepath=csvdataDirectory   
                                    )
        import_new_data = PythonOperator(
                                task_id='import_new_data',
                                python_callable=import_to_mysql
                                    )
        delete_temporary_files = PythonOperator(
                                task_id='delete_temporary_files',
                                python_callable=delete_temporary_files
                                    )
        no_new_files = PythonOperator(
                                task_id='no_new_files',
                                python_callable=no_new_files
                                    )
        finish_processing = PythonOperator(
                                task_id='finish_processing',
                                python_callable=finish_processing,
                                trigger_rule='none_failed_or_skipped'    
                                    )      


        is_dropbox_Mysql_available >> check_for_new_files >> download_data >> process_data >> detect_import_file >> import_new_data >> delete_temporary_files >> finish_processing
        is_dropbox_Mysql_available >> check_for_new_files >> no_new_files >> finish_processing

