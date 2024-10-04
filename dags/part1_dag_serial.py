import pandas as pd
import requests
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


# Function that loads leads from SQL Server and stores them in a temporary .csv file
def mssql_load_leads(ti):
    filename = "leads.csv"
    hook = MsSqlHook(mssql_conn_id='leads_db')
    df = hook.get_pandas_df(sql="SELECT * FROM dbo.AspNetUsers;")
    df.to_csv(filename, sep=',', index=False, encoding='utf-8')
    ti.xcom_push(key="filename", value=filename)
    
    # Should handle exceptions before claiming victory!
    print("Saved {0} Leads to file {1}.".format(len(df.index), filename))

# Function that removes any leads who asked not to be contacted
def file_clean_leads(ti):
    filename = ti.xcom_pull(key="filename", task_ids="ExtractLeads")
    results_filename = "contacts.csv"
    df = pd.read_csv(filename)
    contacts_df = df[(df.UserName != "test")]
    contacts_df.to_csv(results_filename, sep=',', index=False, encoding='utf-8')
    ti.xcom_push(key="filename", value=results_filename)

    # Should handle exceptions before claiming victory!
    print("Removed {0} lead(s) and saved {1} to file {2}.".format(len(df.index)-len(contacts_df.index) ,len(contacts_df.index), results_filename))

# Function that saves an updated set of leads to Google sheet
def google_load_contacts(ti):
    filename = ti.xcom_pull(key="filename", task_ids="CleanLeads")
    df = pd.read_csv(filename)
    url = 'https://script.google.com/macros/s/AKfycbxvz3wSa8VGqqjiZyaXitJtkp7sd8maGxUP4kwtKHajFQsep9rKPNAdC5svyNBPyVE5RA/exec'
    
    for index, lead in df.iterrows():
        current_lead = {'Username' : lead['UserName']}
        x = requests.post(url, data = current_lead)

        # Should handle exceptions before claiming victory!
        print("{0} lead(s) saved to GoogleSheet, of {1}.".format(index+1, len(df.index)))    
    

# Create the DAG
dag = DAG(
        dag_id='part1_dag_serial',
        default_args={'owner' : '⭐UP', 'start_date' : datetime(2023, 8, 31)},
        schedule_interval= '@daily', # '*/5 * * * *', 
        catchup=False
    )

# Task 1
# Connect to SQL Server and pull all records
MSSQL_load_leads = PythonOperator(
    task_id='ExtractLeads',
    python_callable=mssql_load_leads,
    dag=dag
)

# Task 2
# Remove duplicates and remove 'DO NOT CONTACT'
python_clean_leads = PythonOperator(
    task_id = 'CleanLeads', 
    python_callable = file_clean_leads,
    dag = dag)

# Task 3
# Append to Google Sheet
google_save_contacts = PythonOperator(
    task_id = 'LoadLeads', 
    python_callable = google_load_contacts,
    dag = dag)

# Dependencies
MSSQL_load_leads >> python_clean_leads >> google_save_contacts