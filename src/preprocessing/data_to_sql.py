import pandas as pd
import os

# pip install SQLAlchemy
# pip install pyyaml
# install MySQL Driver
# pip install PyMySQL


def get_data(connection, db_name, table_name):
    ''' Get the dataset of the company with longest duration'''
    query = f"""
            SELECT *
            FROM {db_name}.{table_name}
            WHERE ticker = (
                SELECT ticker from {db_name}.{table_name} 
                GROUP BY ticker
                ORDER BY (MAX(date) - MIN(date))
                DESC
                LIMIT 1);
        """
    try:
        # Convert the selected data to df
        df = pd.read_sql_query(query, connection)
        if not df.empty:
            return df
        else:
            print('Data not found!')
    except Exception:
        raise


def insert_to_sql(folder_path, engine, table_name):
    '''Insert all data from csv files to a database'''
    # Loop through all CSV files in the folder and combine them
    try:
        with engine.begin() as connection:  # Begin transaction
            for file in os.listdir(folder_path):
                if file.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(os.path.join(
                            folder_path, file), on_bad_lines='warn')
                        # Create a column of ticker name from filename
                        company_ticker = file.split('.')[0].split('-')[0]
                        df['Ticker'] = company_ticker
                        try:
                            # Convert date data
                            if "TradingDate" in df.columns:
                                df['date'] = pd.to_datetime(df['TradingDate'],
                                                            dayfirst=True,
                                                            errors='coerce')
                            elif "Date" in df.columns:
                                df["date"] = pd.to_datetime(df["Date"],
                                                            dayfirst=True,
                                                            errors='coerce')
                            else:
                                print(f"Column Date not found {file}, skip")
                                # raise
                                continue
                            # Check for bad dates
                            invalid_dates = df[df['date'].isna()]
                            if not invalid_dates.empty:
                                print(
                                    f'Warning: {len(invalid_dates)} invalid dates in {file}')
                            # Drop row with missing date
                            df.dropna(subset=['date'], inplace=True)
                            
                        except Exception as e:
                            print(f"Error converting Date for {file}: {e}")
                            # raise
                            continue
                        # Rename columns to align with the table in SQL DB
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
                        # Insert data to SQL database
                        df.to_sql(table_name, con=connection,
                                  if_exists='append', index=False)
                        print(
                            f"Inserted data for {company_ticker} into DB.")
                    except Exception:
                        raise
            print("Data insertion complete.")
    except FileNotFoundError:
        raise
    except Exception as e:
        print(f'Error during insertion: {e}')
        raise
