import pandas as pd
import os
import yaml
from utils import db_connection

# pip install SQLAlchemy
# pip install pyyaml
# install MySQL Driver
# pip install PyMySQL

'''
# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Database connection setup
db_config = config['database']
engine = create_engine(f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
'''

config = db_connection.load_config()
engine = db_connection.get_db_engine(config)
# Path to the folder containing your CSV files
folder_path = config['data']['folder_path']


def
# Loop through all CSV files in the folder and combine them
try:
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join(folder_path, file))
                company_ticker = file.split('-')[0]
                df['Ticker'] = company_ticker
                df['TradingDate'] = pd.to_TradingDatetime(df['TradingDate'])
                df.rename(columns={'TradingDate': 'tradingdate', 'Open': 'open',
                                   'High': 'high', 'Low': 'low', 'Close': 'close',
                                   'Volume': 'volume'}, inplace=True)
                df.to_sql('stockdb', con=engine,
                          if_exists='append', index=False)
                print(f"Inserted data for {company_ticker} into the database.")
            except Exception as e:
                print(f'Error: {e}')
    print("Data insertion complete.")
except FileNotFoundError as e:
    print(f'File not found: {e}')


'''
import mysql.connector
connection = mysql.connector.connect(
    user="root",
    password ="",
    host="",
    database="",
    ssl_disablrd=True
)
cursor = connection.cursor()
query = select * from open;
cursor.excecute(query)
results = []
for i, data in enumerate(cursor):
    results.append(data)
cursor.close()
connection.close()
df = pd.DataFrame(results)
'''
