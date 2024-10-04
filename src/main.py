# import pandas as pd
from utils.db_connection import load_config, get_db_engine, get_db_connection
'''Encounter Wildcard imports when trying to
from utils.db_connection import * '''
from preprocessing.data_to_sql import insert_to_sql
import pandas as pd


config = load_config()
engine = get_db_engine(config)
# Path to the folder containing your CSV files
folder_path = config['data']['test_path']
db_name = config["database"]["dbname"]
table_name = config["tables"]["testnasdag"]
insert_to_sql(folder_path, engine, db_name, table_name)
'''
# Perform preprocessing (get data of oldest company) on sql
connection = get_db_connection()
cursor = connection.cursor()
cursor.Execute(query)
results = []
for i, data in enumerate(cursor):
    results.append(data)
cursor.close()
connection.close()
df = pd.DataFrame(results)
'''