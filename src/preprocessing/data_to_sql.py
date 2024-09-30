import pandas as pd
import os

# pip install SQLAlchemy
# pip install pyyaml
# install MySQL Driver
# pip install PyMySQL


def insert_to_sql(folder_path, engine, table_name):
    # Loop through all CSV files in the folder and combine them
    try:
        for file in os.listdir(folder_path):
            if file.lower().endswith('.csv'):
                try:
                    df = pd.read_csv(os.path.join(folder_path, file))
                    company_ticker = file.split('-')[0]
                    df['Ticker'] = company_ticker
                    try:
                        if "TradingDate" in df.columns:
                            df['Date'] = pd.to_datetime(df['TradingDate'],
                                                        dayfirst=True)
                        elif "Date" in df.columns:
                            df["Date"] = pd.to_datetime(df["Date"],
                                                        dayfirst=True)
                        else:
                            print(f"Column Date not found {file}, skip")
                            continue
                    except Exception as e:
                        print(f"Error converting Date for {file}: {e}")
                        continue  # skip this file

                    df.rename(columns={'TradingDate': 'date',
                                       'Ticker': 'ticker',
                                       'Open': 'open', 'High': 'high',
                                       'Low': 'low', 'Close': 'close',
                                       'Volume': 'volume',
                                       'AdjustedClose': 'adjustedclose'},
                              inplace=True)
                    df.to_sql(table_name, con=engine,
                              if_exists='append', index=False)
                    print(
                        f"Inserted data for {company_ticker} into database.")
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
