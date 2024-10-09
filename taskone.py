
#import required libraries to connect to GBQ
import os
import re
import datetime 

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas_gbq
import janitor
import zipfile36 as zipfile

from google.cloud import bigquery
from google.oauth2 import service_account

#set the path to the credentials file 
service_path = "C:/Users/thaefele31/Documents/ADA/Assignments/WedgeProject/"
service_file = 'wedgeproject-438019-d3b6147b9c41.json' 
gbq_proj_id = 'wedgeproject-438019' 

# set the credentials
private_key =service_path + service_file

#set the credentials
credentials = service_account.Credentials.from_service_account_file(private_key)

#establish the connection to the GBQ
client = bigquery.Client(credentials = credentials, project=gbq_proj_id)

#Read in the data from zip file

#set the path to the data
path = "C:/Users/thaefele31/Documents/ADA/Assignments/WedgeProject/Data/"

#create empty dataframe
df = pd.DataFrame()

#unzip the file
#with zipfile.ZipFile(path + "wedge-clean-files.zip", 'r') as zip_ref:
    #zip_ref.extractall(path)

#list to store dataframes
dataframes = []


# Read in a CSV from the unzipped folder to test
csv_folder_path = path + "clean-files/"

dataframes_dict = {}

# Define chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust based on your system's memory capacity

# Loop through all files in the folder
for filename in os.listdir(csv_folder_path):
    if filename.endswith(".csv"):  # Check if the file is a CSV
        file_path = os.path.join(csv_folder_path, filename)
        try:
            chunk_list = []  # Store chunks of the current file
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                chunk_list.append(chunk)  # Add each chunk to the chunk list
                print(f"Processed a chunk of size {chunk.shape[0]} rows from {filename}")
            
            # Concatenate all chunks of the current file into one DataFrame
            file_df = pd.concat(chunk_list, ignore_index=True)
            # Store the DataFrame in the dictionary with the filename (without extension) as the key
            dataframes_dict[filename[:-4]] = file_df

            print(f"Successfully read and processed all chunks of: {filename}")
        except Exception as e:
            print(f"Failed to read {filename}: {e}")

# Optionally, print the keys of the dictionary to confirm the DataFrames are stored
print("\nDataFrames stored in dictionary with the following keys:")
print(list(dataframes_dict.keys()))

