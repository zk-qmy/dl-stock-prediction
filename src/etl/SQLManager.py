import yaml
import sys
import pandas as pd
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
        db_config = self.config['database']
        try:
            engine = create_engine(f"mysql+pymysql://"
                                   f"{db_config['username']}:"
                                   f"{db_config['password']}@"
                                   f"{db_config['host']}:{db_config['port']}/"
                                   f"{db_config['dbname']}")
            return engine
        except exc.SQLAlchemyError as e:
            logging.info(f"Database Connecting failed: {e}")
            raise CustomException(e, sys)

    def get_db_connection(self):
        print("get into get_db_connection!")
        if self.engine is None:
            self.engine = self.get_db_engine()

        try:
            connection = self.engine.connect()
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
                # Log the query for debugging
                logging.info(f"Generated query:\n{create_table_query}")
                logging.info(f"Table {table_name} created!")
                return True
            except Exception as e:
                logging.info(f"Error creating table `{table_name}`: {e}")
                return False

    def insert_data_from_df(self, df, table_name):
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
                index=False)
            logging.info(f"Data inserted into `{table_name}` successfully!")
            return True
        except Exception as e:
            logging.info(f"Error inserting data into `{table_name}`: {e}")
            return False

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
        "hire_date": pd.to_datetime(["2020-01-01", "2019-05-15", "2018-07-30", "2021-02-10", "2011-01-20"])
    })

    table_name = "employees"
    manager = SQLManager()
    if (manager.create_table_from_df(df, table_name)):
        print("Successfully create table")
    if (manager.insert_data_from_df(df, table_name)):
        print("Successfully insert data!")
