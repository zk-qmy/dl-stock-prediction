import pandas as pd
import os
from datetime import datetime
from sqlalchemy import inspect
from src.Logger import Logger

# pip install SQLAlchemy
# pip install pyyaml
# pip install MySQL Driver
# pip install PyMySQL

logging = Logger("dataToSQL")


def get_data(connection, db_name, table_name):
    """ Get the dataset of the company with longest duration"""
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
            logging.log_info("get_data() - dataFrame Empty!")
            # print("Data not found!")
    except Exception as e:
        logging.log_error(f"get_data() - Exception: {e}")
        raise


def insert_to_sql(folder_path, engine, table_name):
    """Insert all data from csv files to a database"""
    # Loop through all CSV files in the folder and combine them
    try:
        with engine.begin() as connection:  # Begin transaction
            inspector = inspect(engine)
            sql_columns = [col["name"]
                           for col in inspector.get_columns(table_name)]

            for file in os.listdir(folder_path):
                if file.lower().endswith(".csv"):
                    try:
                        df = pd.read_csv(os.path.join(
                            folder_path, file), on_bad_lines="warn")
                        # Skip empty dataframes
                        if df is None or df.empty:
                            print(f"{file} is empty. Skipping!")
                            continue

                        # Create a column of ticker name from filename
                        company_ticker = file.split(".")[0].split("-")[0]
                        df["Ticker"] = company_ticker
                        isDateColEmpty = False
                        try:
                            # Convert date data
                            if "TradingDate" in df.columns:
                                df["date"] = pd.to_datetime(df["TradingDate"],
                                                            dayfirst=False,
                                                            log_errors="coerce")
                                df.drop(columns=["TradingDate"], inplace=True)
                            elif "Date" in df.columns:
                                df["date"] = pd.to_datetime(df["Date"],
                                                            dayfirst=False,
                                                            log_errors="coerce")
                                df.drop(columns=["Date"], inplace=True)
                            else:
                                isDateColEmpty = True
                                print(f"Column Date not found {file}, skip")
                                # raise
                                continue
                            if isDateColEmpty:
                                continue
                            else:
                                df["date"] = df["date"].dt.strftime(
                                    r"%Y-%m-%d")
                            drop_invalid_date(df, file)
                        except Exception as e:
                            print(f"log_error converting Date for {file}: {e}")
                            # raise
                            continue
                        # Rename columns to align with the table in SQL DB
                        df.rename(columns={"TradingDate": "date",
                                           "Ticker": "ticker",
                                           "Open": "open", "High": "high",
                                           "Low": "low", "Close": "close",
                                           "Volume": "volume",
                                           "Adjusted Close": "adjustedclose"},
                                  inplace=True)
                        new_column_order = ["date", "ticker", "low", "open",
                                            "volume", "high", "close",
                                            "adjustedclose"]
                        # Check the DataFrame before insertion
                        # print(f"Final cols to insert in {file}: {df.columns.tolist()}")

                        matching_cols = get_matching_cols(df, sql_columns,
                                                          new_column_order,
                                                          file)
                        # Check if the DataFrame is empty
                        if not matching_cols:
                            print(f"No matching columns for {file}. Skipping.")
                            continue
                        df = df[matching_cols]
                        insert_df_to_sql(df, table_name,
                                         connection, file,
                                         company_ticker=company_ticker)

                    except Exception as e:
                        logging.log_error(f"insert_to_sql()- Exception: {e}")
                        raise
            print("Data insertion complete.")
    except FileNotFoundError as e:
        logging.log_error(f"insert_to_sql() - FileNotFound: {e}")
        raise
    except Exception as e:
        logging.log_error(
            f"insert_to_sql() - Exception: log_error during insertion {e}")
        # print(f"log_error during insertion: {e}")
        raise


def insert_df_to_sql(df, table_name, connection, file, company_ticker=None):
    """Insert data to SQL database"""
    if not df.empty:
        df.to_sql(table_name, con=connection,
                  if_exists="append", index=False)
        print(
            f"Inserted data for {company_ticker} into DB.")
    else:
        logging.log_info(
            "insert_df_to_sql() - DF is empty"
            + f"after processing {file}. Skipping!")
        # print(f"DF is empty after processing {file}. Skipping!")


def get_matching_cols(df, sql_columns, new_column_order, file):
    """Handle case sensitive for matching columns"""
    sql_cols = [col.lower() for col in sql_columns]
    df_cols = [col.lower() for col in df.columns]
    matching_cols = []
    for col in new_column_order:
        if col.lower() in sql_cols and col.lower() in df_cols:
            matching_cols.append(col)
    return matching_cols


def drop_invalid_date(df, file):
    # Drop duplicate columns
    # print(
    #    f"Columns after date conversion in {file}: {df.columns.tolist()}")
    df = df.loc[:, ~df.columns.duplicated()]
    # print(
    #    f"Columns after dropping duplicate in {file}: {df.columns.tolist()}")

    # Check for bad dates
    invalid_dates = df[df["date"].isna()]
    if not invalid_dates.empty:
        logging.log_info(
            "drop_invalid_date() - Warning: "
            + f"{len(invalid_dates)} invalid dates in {file}")
        # print(
        #    f"Warning: {len(invalid_dates)} invalid dates in {file}")
    # Drop row with missing date
    df.dropna(subset=["date"], inplace=True)
