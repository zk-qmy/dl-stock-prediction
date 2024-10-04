# import pandas as pd
from utils.db_connection import load_config, get_db_engine
'''Encounter Wildcard imports when trying to
from utils.db_connection import * '''
from preprocessing.data_to_sql import insert_to_sql, get_data
import os

config = load_config()
engine = get_db_engine(config)
# Path to the folder containing CSV files
folder_path = config['data']['test_path']
db_name = config["database"]["dbname"]
table_name = config["tables"]["testnasdag"]

insert_to_sql(folder_path, engine, db_name, table_name)
with engine.connect() as connection:
    df = get_data(connection, db_name, table_name)
print(df)


# Specify the directory
directory = 'data\\toprocess'

# Create directory if it does not exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Save DataFrame as CSV
df.to_csv(os.path.join(directory, 'nasdag_chosen.csv'), index=False)
