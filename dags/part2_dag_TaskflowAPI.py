import pandas as pd
import requests
from airflow import DAG
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

@dag(
    start_date=datetime(2023, 8, 31),
    schedule='@daily', # '*/5 * * * *', 
    catchup=False,
)
def starUP_leads_taskflow():

    @task()
    def extractLeads():
        filename = "leads.csv"
        hook = MsSqlHook(mssql_conn_id='leads_db')
        df = hook.get_pandas_df(sql="SELECT * FROM dbo.AspNetUsers;")
        df.to_csv(filename, sep=',', index=False, encoding='utf-8')
        
        # Should handle exceptions before claiming victory!
        print("Saved {0} Leads to file {1}.".format(len(df.index), filename))

        return filename

    @task()
    def cleanLeads(filename: str):
        results_filename = "contacts.csv"
        df = pd.read_csv(filename)
        contacts_df = df[(df.UserName != "test")]
        contacts_df.to_csv(results_filename, sep=',', index=False, encoding='utf-8')
        
        # Should handle exceptions before claiming victory!
        print("Removed {0} lead(s) and saved {1} to file {2}.".format(len(df.index)-len(contacts_df.index) ,len(contacts_df.index), results_filename))

        return results_filename

    @task()
    def loadLeads(filename: str):
        df = pd.read_csv(filename)
        url = 'https://script.google.com/macros/s/AKfycbxvz3wSa8VGqqjiZyaXitJtkp7sd8maGxUP4kwtKHajFQsep9rKPNAdC5svyNBPyVE5RA/exec'
        
        for index, lead in df.iterrows():
            current_lead = {'Username' : lead['UserName']}
            x = requests.post(url, data = current_lead)

            # Should handle exceptions before claiming victory!
            print("{0} lead(s) saved to GoogleSheet, of {1}.".format(index+1, len(df.index)))    

    loadLeads(cleanLeads(extractLeads()))
    
starUP_leads_taskflow()