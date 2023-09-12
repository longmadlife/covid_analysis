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


QUERY = (
    "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` "
    "WHERE state = 'TX' "
    "LIMIT 100")
query_job = client.query(QUERY)  
rows = query_job.result()  
for row in rows:
    print(row.name)

df = pd.read_csv('covid_data.csv')
df = df[df.states == 56]
    #remove unnecessary columns
df = df.drop(columns=["recovered", "lastModified", "states", "dateChecked", "total", "posNeg", "hospitalized"])    # Reorder columns
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
print(df.columns)

dataset_name = "test"
#client.create_dataset(dataset_name)     #create dataset
dataset = client.get_dataset(dataset_name)         # get dataset

table_name = "data_Test"
# get project id
project_id = dataset.project
dataset_id = dataset.dataset_id
table_id = project_id + '.' + dataset_id + '.' + table_name

#client.create_table(table_id) # create table
table = client.get_table(table_id)           # get table
#print(table.self_link)


       
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
load_job  = client.load_table_from_dataframe(
            df, 
            table, 
            location="US",
            job_config=job_config)  

load_job.result()  
print("Loaded {} rows into {}:{}.".format(load_job.output_rows, dataset_name, table_name))
