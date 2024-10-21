import os
import pandas as pd
import numpy as np
import pandas_gbq
import zipfile36 as zipfile
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound 


#cleaning the data
def clean_data(df):

    #assign false if null or empty
    df['memType'] = df['memType'].fillna(False)  # fill with False
    df['staff'] = df['staff'].fillna(False)      # fill with False
    df['batchHeaderID'] = df['batchHeaderID'].fillna(False)  # fill with False
    df['display'] = df['display'].fillna(False)  # fill with False

    # Now, convert to boolean type
    df['memType'] = df['memType'].astype(bool)
    df['staff'] = df['staff'].astype(bool)
    df['batchHeaderID'] = df['batchHeaderID'].astype(bool)
    df['display'] = df['display'].astype(bool)


    #convert to datetime
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    # define float and string columns
    float_columns = {
    'register_no': 'float',
    'emp_no': 'float',
    'trans_no': 'float',
    'department': 'float',
    'quantity': 'float',
    'Scale': 'float',
    'cost': 'float',
    'unitPrice': 'float',
    'total': 'float',
    'regPrice': 'float',
    'altPrice': 'float',
    'tax': 'float',
    'taxexempt': 'float',
    'foodstamp': 'float',
    'wicable': 'float',
    'discount': 'float',
    'memDiscount': 'float',
    'discountable': 'float',
    'discounttype': 'float',
    'voided': 'float',
    'percentDiscount': 'float',
    'ItemQtty': 'float',
    'volDiscType': 'float',
    'volume': 'float',
    'VolSpecial': 'float',
    'mixMatch': 'float',
    'matched': 'float',
    'numflag': 'float',
    'itemstatus': 'float',
    'tenderstatus': 'float',
    'varflag': 'float',
    'local': 'float',
    'organic': 'float',
    'receipt': 'float',
    'card_no': 'float',
    'store': 'float',
    'branch': 'float',
    'match_id': 'float',
    'trans_id': 'float'
}

    string_columns = {
        'upc': 'string',
        'description': 'string',
        'trans_type': 'string',
        'trans_subtype': 'string',
        'trans_status': 'string',
        'charflag': 'string'
    }

    # Step 2: Handle nulls for float and string columns separately

    # Replace nulls in float columns 
    for column in float_columns.keys():
        df[column] = df[column].fillna(np.nan)
        df[column] = df[column].replace(["\\N", " ",""], np.nan)
    
    # Replace nan in string columns with null
    for column in string_columns.keys():
        df[column] = df[column].fillna(np.nan)
        df[column] = df[column].replace(["\\N", "0"], np.nan)

    # Step 3: Apply the astype conversion

    # Convert float columns
    for column, dtype in float_columns.items():
        df[column] = df[column].astype(dtype)

    # Convert string columns
    for column, dtype in string_columns.items():
        df[column] = df[column].astype(dtype)

    return df


#function upload to database
def upload_to_bigquery(df,table_name):
    print('Uploading data to the cloud...')

    #setup connection with user default credentials
    project_id = 'wedgeproject-438019'

#setup connection with user default credentials
    client = bigquery.Client(project=project_id)


    #Setup schema for the data
    schema =[
        {"name": "datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "register_no", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "emp_no", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "trans_no", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "upc", "type": "STRING", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "trans_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "trans_subtype", "type": "STRING", "mode": "NULLABLE"},
        {"name": "trans_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "department", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "quantity", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "Scale", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "unitPrice", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "total", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "regPrice", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "altPrice", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "tax", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "taxexempt", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "foodstamp", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "wicable", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "discount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "memDiscount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "discountable", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "discounttype", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "voided", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "percentDiscount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ItemQtty", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volDiscType", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volume", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "VolSpecial", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "mixMatch", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "matched", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "memType", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "staff", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "numflag", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "itemstatus", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "tenderstatus", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "charflag", "type": "STRING", "mode": "NULLABLE"},
        {"name": "varflag", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "batchHeaderID", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "local", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "organic", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "display", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "receipt", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "card_no", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "store", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "branch", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "match_id", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "trans_id", "type": "FLOAT", "mode": "NULLABLE"}
    ]

    #set job config
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Appends to existing table
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],  # Allow new fields
        schema=schema,  # Optional schema definition
    )

    print("Checking if table exists...")

    # Check if table exists and remove it if it does
    try:
        client.get_table(table_name)
        print(f"Deleting table {table_name}...")
        client.delete_table(table_name)
    except NotFound:
        print(f"Table {table_name} does not exist.")

    # Load DataFrame to BigQuery (without saving locally)
    job = client.load_table_from_dataframe(df, table_name, job_config=job_config)

    # Wait for the load job to complete
    job.result()

    # Error handling
    if job.errors:
        print(f"Error loading {df}: {job.errors}")
    else:
        print(f"Successfully loaded {job.output_rows} rows into {table_name}")

    







