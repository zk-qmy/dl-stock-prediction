'''Get top profitable company in the industry and bottom company
in the industry based on some indicator in industry analysis, then
choose the the company with best quality data of each group (profit and risk)
to train and get the top and bottom company '''
import pandas as pd
get_db_engine
from .csv_prep import data_to_sql as dtsql
import os
# from src.Logger import Logger

# logging = Logger("prepare_data")
'''
config = load_config()
engine = get_db_engine(config)
# Path to the folder containing CSV files
folder_path = config['data']["test_path_ind_an"]  # test_path: 'test_path_ind_an'
db_name = config["database"]["dbname"]
table_name = config["tables"]["testindustry"]  # test_table: 'testindustry'

'''

# Prepare industry analysis raw data


def prepare_raw_data(folder_path, engine, table_name):
    """Insert all data from csv files to a database"""
    # Loop through all CSV files in the folder and combine them
    try:
        with engine.begin() as connection:  # Begin transaction
            for file in os.listdir(folder_path):
                if file.lower().endswith(".csv"):
                    try:
                        df = pd.read_csv(os.path.join(
                            folder_path, file), on_bad_lines="warn")
                        # Skip empty dataFrames
                        if df is None or df.empty:
                            print(f"{file} is empty. Skipping!")
                            continue
                        dtsql.insert_df_to_sql(df,
                                               table_name,
                                               connection,
                                               file)
                    except Exception:
                        # logging.log_error(f"insert_to_sql()- Exception: {e}")
                        raise
            print("Data insertion complete.")
    except FileNotFoundError:
        # logging.log_error(f"insert_to_sql() - FileNotFound: {e}")
        raise
    except Exception:
        # logging.log_error(
        #    f"insert_to_sql() - Exception: log_error during insertion {e}")
        # print(f"log_error during insertion: {e}")
        raise


def get_raw_top_data(connection, db_name, table_name):
    """ Get the dataset of the company with longest duration"""
    query = f"""
            SELECT *
            FROM {db_name}.{table_name}
            WHERE ticker = (
                SELECT ticker from {db_name}.{table_name}
                GROUP BY ticker
                ORDER BY (MAX(date) - MIN(date))
                DESC
                LIMIT 5);
        """
    try:
        # Convert the selected data to df
        df = pd.read_sql_query(query, connection)
        if not df.empty:
            return df
        else:
            # logging.log_info("get_data() - dataFrame Empty!")
            print("Data not found!")
    except Exception:
        # logging.log_error(f"get_data() - Exception: {e}")
        raise


'''
prepare_raw_data(folder_path, engine, table_name)
with engine.connect() as connection:
    df = get_data(connection, db_name, table_name)
print(df)


# Specify the directory
directory = 'data\\toprocess'

# Create directory if it does not exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Save DataFrame as CSV
df.to_csv(os.path.join(directory, 'ind_test_selected.csv'), index=False)
'''
