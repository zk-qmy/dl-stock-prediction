# import pandas as pd
from src.utils.db_connection import load_config, get_db_engine
'''Encounter Wildcard imports when trying to
from utils.db_connection import * '''
#from src.etl.data_to_sql import insert_to_sql, get_data
from src.etl.prepare_data import prepare_raw_data, get_raw_top_data
import os

config = load_config()
engine = get_db_engine(config)
'''
# Path to the folder containing CSV files
folder_path = config['data']["test_path_vn"]  # Change to  a test path in 'data' field for test  'vn_230228_path'
db_name = config["database"]["dbname"]
table_name = config["tables"]["test"]  # Change to a test table in 'tables' field for test DB

insert_to_sql(folder_path, engine, table_name)
with engine.connect() as connection:
    df = get_data(connection, db_name, table_name)
print(df)


# Specify the directory
directory = 'data\\toprocess'

# Create directory if it does not exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Save DataFrame as CSV
df.to_csv(os.path.join(directory, 'vn_test_selected.csv'), index=False)

'''

# Path to the folder containing CSV files
folder_path = config['data']["test_path_ind_an"]  # test_path: 'test_path_ind_an'
db_name = config["database"]["dbname"]
table_name = config["tables"]["testindustry"]  # test_table: 'testindustry'
prepare_raw_data(folder_path, engine, table_name)
with engine.connect() as connection:
    df = get_raw_top_data(connection, db_name, table_name)
print(df)


# Specify the directory
directory = 'data\\toprocess'

# Create directory if it does not exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Save DataFrame as CSV
df.to_csv(os.path.join(directory, 'ind_test_selected.csv'), index=False)

# python -m src.main
