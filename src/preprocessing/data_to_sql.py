import pandas as pd
import os
from sqlalchemy import text

# pip install SQLAlchemy
# pip install pyyaml
# install MySQL Driver
# pip install PyMySQL


def get_data():
    ''' Get the dataset of the oldest company'''


def insert_to_sql(folder_path, engine, db_name, table_name):
    '''Insert all data from csv files to a database'''
    # Loop through all CSV files in the folder and combine them
    try:
        with engine.begin() as connection:
            for file in os.listdir(folder_path):
                if file.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(os.path.join(folder_path, file))
                        company_ticker = file.split('.')[0].split('-')[0]
                        df['Ticker'] = company_ticker
                        try:
                            if "TradingDate" in df.columns:
                                df['date'] = pd.to_datetime(df['TradingDate'],
                                                            dayfirst=True)
                            elif "Date" in df.columns:
                                df["date"] = pd.to_datetime(df["Date"],
                                                            dayfirst=True)
                            else:
                                print(f"Column Date not found {file}, skip")
                                raise
                        except Exception as e:
                            print(f"Error converting Date for {file}: {e}")
                            raise

                        df.rename(columns={'TradingDate': 'date',
                                           'Ticker': 'ticker',
                                           'Open': 'open', 'High': 'high',
                                           'Low': 'low', 'Close': 'close',
                                           'Volume': 'volume',
                                           'Adjusted Close': 'adjustedclose'},
                                  inplace=True)
                        new_column_order = ['date', 'ticker', 'low', 'open',
                                            'volume', 'high', 'close',
                                            'adjustedclose']
                        df = df[new_column_order]
                        df.to_sql(table_name, con=connection,
                                  if_exists='append', index=False)
                        print(
                            f"Inserted data for {company_ticker} into DB.")
                    except Exception:
                        # print(f'Error: {e}')
                        raise
            print("Data insertion complete.")
            query = f"""
                SELECT ticker
                FROM {db_name}.{table_name}
                WHERE date = (SELECT MIN(date) FROM {db_name}.{table_name} );
            """
            try:
                result = connection.execute(text(query))
                tickers = result.fetchall()
                if tickers:
                    print(tickers)
                else:
                    print('sdjfnsnot found')
            except Exception:
                raise
    except FileNotFoundError:
        # print(f'File not found: {e}')
        raise
