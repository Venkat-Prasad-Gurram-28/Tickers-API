import json
import requests
import os
import pandas as pd
import awswrangler as wr

def get_data(url):
    response = requests.get(url)
    return response.json()

def create_anthena_table(df, table_name, db, s3_path):
    wr.s3.to_parquet(
    df=df,
    path= s3_path,
    dataset=True,
    database= db,
    table = table_name,
    mode="overwrite"
    )  

def lambda_handler(event, context):
    
    print("event", event)
    print("context", context)
    url = 'https://h5hqxa4e7j.execute-api.us-east-1.amazonaws.com/api/v5/market/tickers'
    response_json = get_data(url)
    data_json = response_json['data']
    data_df = pd.DataFrame(data_json)
    data_length =  len(data_df)
    create_anthena_table(data_df, 'tickers', 'mydb28', 's3://temp-bucket-28/glue_temp/')
    event['Payload'] = {'database' : "mydb28",
                            'table_name' : 'tickers',
                            'data_length' : str(data_length),
                            'status' : "SUCCEEDED"
                        } 
    print(event)

    return event
