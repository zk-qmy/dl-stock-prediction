# import pandas as pd
from utils.db_connection import load_config, get_db_engine 
'''Encounter Wildcard imports when trying to
from utils.db_connection import * '''
from preprocessing.data_to_sql import insert_to_sql

config = load_config()
engine = get_db_engine(config)
# Path to the folder containing your CSV files
folder_path = config['data']['folder_path']
insert_to_sql(folder_path, engine, config["tables"]["nasdag"])
