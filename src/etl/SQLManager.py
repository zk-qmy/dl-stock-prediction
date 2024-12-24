import yaml
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, exc, text
from src.Logger import logging
from src.Exception import CustomException


class SQLConnection:
    def __init__(self, config_path="config.yaml"):
        print("init SQLConnection")
        self.config = self.load_config(config_path)
        self.engine = None

    def load_config(self, config_path):
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError as e:
            logging.info(f"Error: {config_path} not found")
            raise CustomException(e, sys)
        except yaml.YAMLError as e:
            logging.info(f"Error reading file: {e}")
            raise CustomException(e, sys)

    def get_db_engine(self):
        # Setup DB connection
        if self.engine is None:
            db_config = self.config['database']
            try:
                self.engine = create_engine(f"mysql+pymysql://"
                                            f"{db_config['username']}:"
                                            f"{db_config['password']}@"
                                            f"{db_config['host']}:{db_config['port']}/"
                                            f"{db_config['dbname']}")
            except exc.SQLAlchemyError as e:
                logging.info(f"Database Connecting failed: {e}")
                raise CustomException(e, sys)
        return self.engine

    def get_db_connection(self):
        print("get into get_db_connection!")
        engine = self.get_db_engine()
        try:
            connection = engine.connect()
            logging.info("Connected to database!")
            print("database connected")
            return connection
        except exc.SQLAlchemyError as e:
            logging.error(f"Database Connection failed: {e}")
            raise CustomException(e, sys)


class SQLManager:
    def __init__(self):
        print("init SQLManager")
        self.connection = SQLConnection().get_db_engine()
        print("Done getting connection from SQL Manager!")

    def check_table_exists(self, table_name):
        query = f"SHOW TABLES LIKE '{table_name}'"
        with self.connection.connect() as conn:
            result = conn.execute(text(query)).fetchone()
        return result is not None

# Metadata table: Insert metadata
    def create_metadata_table(self, table_name="crawl_metadata"):
        if self.check_table_exists(table_name):
            logging.info(f"Table `{table_name}` already exists.")
            return True

        query = """CREATE TABLE crawl_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            last_crawl_date DATE
            );
            """
        # Execute query
        with self.connection.connect() as conn:
            try:
                conn.execute(text(query))
                logging.info(f"Table `{table_name}` created!")
                return True
            except Exception as e:
                logging.info(f"Error creating table `{table_name}`: {e}")
                return False

    def update_metadata_table(self, last_crawl_date,
                              table_name="crawl_metadata"):
        # Check if table exist
        if not self.check_table_exists(table_name):
            logging.info(f"Table `{table_name}` not exist.")
            return False

        # Check date format and ensure it's a datetime object
        '''
        try:
            # If input is a string, convert it to a datetime object
            if isinstance(last_crawl_date, str):
                last_crawl_date = datetime.strptime(
                    last_crawl_date, "%Y-%m-%d")
                logging.info(f"last_crawl_date after conversion: {last_crawl_date}")
        except ValueError as e:
            logging.error(f"Invalid date format: {e}")
            raise CustomException(f"Invalid date format: {e}", sys)'''
        # Ensure last_crawl_date is in the correct format (%Y-%m-%d)
        if isinstance(last_crawl_date, str):
            # Try converting the string to datetime
            try:
                last_crawl_date = datetime.strptime(last_crawl_date, "%Y-%m-%d")
                logging.info(f"After str conversion:{last_crawl_date} ")
            except ValueError as e:
                logging.error(f"Invalid date format {last_crawl_date}: {e}")
                raise CustomException(f"Invalid date format: {e}", sys)
        elif isinstance(last_crawl_date, datetime):
            # If it's already a datetime object, ensure it's in the correct format
            last_crawl_date = last_crawl_date.strftime("%Y-%m-%d")
            logging.info(f"After object conversion: {last_crawl_date}")
        else:
            logging.error(f"Invalid type for last_crawl_date: {type(last_crawl_date)}")
            raise CustomException(f"Invalid type for last_crawl_date: {type(last_crawl_date)}", sys)

        if (last_crawl_date is None):
            logging.info("last_crawl_date is `None`")
            return False
        # Create query
        query = f"""INSERT INTO {table_name} (last_crawl_date)
            VALUES (:last_crawl_date);
            """
        # Execute query
        with self.connection.connect() as conn:
            try:
                result = conn.execute(text(query),
                                      {'last_crawl_date': last_crawl_date})
                if result.rowcount > 0:
                    conn.commit()
                    logging.info(f"Successfully inserted last_crawl_date into `{table_name}`")
                else:
                    logging.warning(f"No rows inserted into `{table_name}`.")
                logging.info(f"Table `{table_name}` updated!")
                return True
            except Exception as e:
                logging.info(f"Error updating table `{table_name}`: {e}")
                conn.rollback()  # Rollback in case of error
                return False

    def fetch_last_crawl_date(self, table_name="crawl_metadata"):
        query = f"""SELECT last_crawl_date FROM {table_name}
            ORDER BY last_crawl_date DESC
            LIMIT 1"""
        # Execute query
        with self.connection.connect() as conn:
            try:
                result = conn.execute(text(query)).fetchone()
                if result:
                    # Assuming the date is the first column in the result
                    latest_crawl_date = result[0]
                    logging.info(f"Latest crawl date: {latest_crawl_date}")
                    return latest_crawl_date
                else:
                    logging.info(f"No data found in table `{table_name}`")
                    return None
            except Exception as e:
                logging.error(f"Error retrieving from `{table_name}`: {e}")
                return None

# Main table: Insert raw data to main table
    def create_table_from_df(self, df, table_name):
        if self.check_table_exists(table_name):
            logging.info(f"Table `{table_name}` already exists.")
            return True

        table_name = table_name.lower()
        # Mapping data type between df and sql
        mapping = {
            "int64": "BIGINT",
            "float64": "FLOAT",
            "object": "VARCHAR(10)",
            "datetime64[ns]": "DATE",
            "bool": "BOOLEAN"
        }
        # Validation
        if df.empty:
            logging.info("Error: DataFrame is empty.")
            return False

        # Create query
        columns = []
        for col, dtype in df.dtypes.items():
            sql_type = mapping.get(str(dtype), "NULL")
            columns.append(f"`{col}` {sql_type}")
        columns_sql = ",\n    ".join(columns)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {columns_sql}
            );
            """
        # Execute query
        with self.connection.connect() as conn:
            try:
                conn.execute(text(create_table_query))
                logging.info("Start creating table `{table_name}` in db")
                logging.info(f"Generated query:\n{create_table_query}")
                logging.info(f"Table `{table_name}` created!")
                return True
            except Exception as e:
                logging.info(f"Error creating table `{table_name}`: {e}")
                return False

    def insert_data_from_df(self, df, table_name, chunksize=1000):
        table_name = table_name.lower()
        if df.empty:
            logging.info("Error: DataFrame is empty.")
            return False

        try:
            # Use SQLAlchemy's `to_sql` for bulk insert
            df.to_sql(
                table_name,
                self.connection,
                if_exists="append",
                index=False,
                chunksize=chunksize)
            logging.info(f"Data inserted into `{table_name}` successfully!")
            return True
        except Exception as e:
            logging.info(f"Error inserting data into `{table_name}`: {e}")
            return False

# Backup table: create backup table
    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Closed db connection!")
            logging.info("Closed database connection!")


if __name__ == "__main__":
    # Sample DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Pudding", "Cake"],
        "salary": [1000.5, 1500.75, 2000.0, 2012.3, 20110.2],
        "is_active": [True, False, True, False, True],
        "hire_date": pd.to_datetime([
            "2020-01-01", "2019-05-15", "2018-07-30",
            "2021-02-10", "2011-01-20"
        ])
    })

    table_name = "employees"
    manager = SQLManager()
    if (manager.create_table_from_df(df, table_name)):
        print("Successfully create table")
    if (manager.insert_data_from_df(df, table_name)):
        print("Successfully insert data!")
    if (manager.create_metadata_table()):
        print("created meta table!")
    if (manager.update_metadata_table("2001-01-23")):
        (print("updated meta table"))
    a = manager.fetch_last_crawl_date()
    if (a):
        print(f"Latest crawl date: {a}")
    if (manager.check_table_exists("crawl_metadata")):
        print("table `crawl_metadata` exist")
