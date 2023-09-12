import requests
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.exc import ProgrammingError
import os

from google.cloud import bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\ttg\Desktop\learn\Analyzing Covid-19\python-bigquery\resonant-triode-356905-59d2c1e75a4d.json"
#project_id = "resonant-triode-356905"
client = bigquery.Client()
def extract_data():
    data = requests.get("https://api.covidtracking.com/v1/us/daily.csv")
    #export the data
    with open('covid_data.csv', 'wb') as f:
        f.write(data.content)
        f.close()
#extract_data()
def transfrom_data():
    df = pd.read_csv('covid_data.csv')
    df = df[df.states == 56]
    #remove unnecessary columns
    df = df.drop(columns=["recovered", "lastModified", "states", "dateChecked", "total", "posNeg", "hospitalized"])
    # Reorder columns
    df = df[["hash", "date", "positive", "negative", "positiveIncrease", "negativeIncrease", "pending", "hospitalizedCurrently", "hospitalizedIncrease", "hospitalizedCumulative",
                "inIcuCurrently", "inIcuCumulative", "onVentilatorCurrently", "onVentilatorCumulative", "totalTestResults", "totalTestResultsIncrease",
                "death", "deathIncrease" 
                ]]
    # Rename columns
    df.rename(columns={
            "negative": "pcr_test_negative",
            "positive": "pcr_test_positive"
        }, inplace=True)
    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])
#df["date"] = pd.to_datetime(df.date.str[0:4]+'-'+df.date.str[4:6]+'-'+df.date.str[6:8])
    df["date"] = df["date"].dt.strftime('%Y-%m-%d')

    # drop nan values
    df = df.dropna()

    # export to csv
    df.to_csv("clean_covid_data.csv")
    
    return df
#create a database
def create_db(dataset_name :str):
    print('Fetching Dataset...')
    # check if data exist 
    try: 
        dataset = client.get_dataset(dataset_name)
        print(f"Dataset '{dataset_name}' already exists.")
        print(dataset.self_link)
        return dataset
    except Exception as e:  
            if e.code == 404:
            # If the dataset doesn't exist, create it
                print(f"Dataset '{dataset_name}' does not exist. Creating a new one...")
                client.create_dataset(dataset_name)     #create dataset
                dataset = client.get_dataset(dataset_name)         # get dataset
                print(f"Database '{dataset_name}' created")
                print(dataset.self_link)
                return dataset
            else:
                print(e)

def create_table(dataset_name:str, table_name :str ):
    
    print(f"fetching table...")
    dataset = create_db(dataset_name)
    # get project id
    project_id = dataset.project
    dataset_id = dataset.dataset_id
    table_id = project_id + '.' + dataset_id + '.' + table_name
    try:
        # check table if exist
        table = client.get_table(table_id)
        print(f"table '{table}' already exists.")
    except Exception as e:
        if e.code == 404:
            print(f"table '{dataset_name}' does not exist. Creating a new one...")
            client.create_table(table_id) # create table
            table = client.get_table(table_id)           # get table
            print(table.self_link)
        else :
            print(e)
    finally:
        return table

def load_data(dataset_name ='covid_19', table_name='covid_data'):
    # get the data
    data = transfrom_data()

    # get table
    table = create_table(dataset_name= dataset_name, table_name= table_name)
        
    job_config = bigquery.LoadJobConfig(
        schema = [
                bigquery.SchemaField("hash", "STRING"),
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("pcr_test_positive", "INTEGER"),
                bigquery.SchemaField("pcr_test_negative", "INTEGER"),
                bigquery.SchemaField("positiveIncrease", "INTEGER"),
                bigquery.SchemaField("negativeIncrease", "INTEGER"),
                bigquery.SchemaField("pending", "INTEGER"),
                bigquery.SchemaField("hospitalizedCurrently", "INTEGER"),
                bigquery.SchemaField("hospitalizedIncrease", "INTEGER"),
                bigquery.SchemaField("hospitalizedCumulative", "INTEGER"),
                bigquery.SchemaField("inIcuCurrently", "INTEGER"),
                bigquery.SchemaField("inIcuCumulative", "INTEGER"),
                bigquery.SchemaField("onVentilatorCurrently", "INTEGER"),
                bigquery.SchemaField("onVentilatorCumulative", "INTEGER"),
                bigquery.SchemaField("totalTestResults", "INTEGER"),
                bigquery.SchemaField("totalTestResultsIncrease", "INTEGER"),
                bigquery.SchemaField("death", "INTEGER"),
                bigquery.SchemaField("deathIncrease", "INTEGER"),
               
            ],
        skip_leading_rows= 1,
        source_format=bigquery.SourceFormat.CSV,
        )
    job_load = client.load_table_from_dataframe(
            data, 
            table, 
            location="US",
            job_config=job_config)  

    job_load.result()
    print("Loaded {} rows into {}:{}.".format(job_load.output_rows, dataset_name, table_name))

if __name__ == "__main__":
    extract_data()
#extract_data()
#transfrom_data()
#load_data()
