import pandas as pd
import os
from sqlalchemy import create_engine
import yaml

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Database connection setup
db_config = config['database']
engine = create_engine(f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

# Path to the folder containing your CSV files
folder_path = config['data']['folder_path']

# Loop through all CSV files in the folder and combine them
for file in os.listdir(folder_path):
    if file.endswith('.csv'):
        df = pd.read_csv(os.path.join(folder_path, file))
        company_name = file.split('.')[0]
        df['company_name'] = company_name
        df['Date'] = pd.to_datetime(df['Date'])
        df.rename(columns={'Date': 'date', 'Open': 'open', 'High': 'high', 
                           'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        df.to_sql('stocks', con=engine, if_exists='append', index=False)
        print(f"Inserted data for {company_name} into the database.")
print("Data insertion complete.")
