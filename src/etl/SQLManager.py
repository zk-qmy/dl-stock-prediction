# from src.utils.db_connection import SQLConnection
from sqlalchemy import create_engine, exc
import yaml
import mysql.connector
from src.Logger import logging
import pandas as pd


class SQLConnection:
    def __init__(self, config_path="config.yaml"):
        print("init SQLConnection")
        self.config = self.load_config(config_path)
        self.connection = None

    def load_config(self, config_path):
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError:
            print(f"Error: {config_path} not found")
            exit(1)
        except yaml.YAMLError as e:
            print(f"Error reading file: {e}")
            exit(1)

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
            print(f"Database Connecting failed: {e}")
            exit(1)

    def get_db_connection(self):
        print("get into get_db_connection!")
        # Check if connection is already established
        if self.connection is not None and self.connection.is_connected():
            print("Connection already established.")
            return self.connection

        try:
            print("start assign config!")
            db_config = self.config['database']
            print(db_config['host'])
            self.connection = mysql.connector.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['username'],
                password=db_config['password'],
                database=db_config['dbname'],
                connection_timeout=60)
            if self.connection.is_connected():
                logging.info("Connected to database!")
                print("Connection successful!")
                return self.connection
            else:
                logging.info("Failed to connect ot MySQL.")
                print("Failed to connect ot MySQL.")
        except mysql.connector.Error as e:
            print(f"Error getting connection!Error: {e}")
            raise Exception  # raise custom exception
        except Exception as e:
            print(f"Exception: {e}")


class SQLManager:
    def __init__(self):
        print("init SQLManager")
        self.connection = SQLConnection().get_db_connection()
        print("Done getting connection from SQL Manager!")

    def create_table_from_df(self, df, table_name):
        # Mapping data type between df and sql
        mapping = {
            "int64": "BIGINT",
            "float64": "FLOAT",
            "object": "VARCHAR(10)",
            "datetime64[ns]": "DATE",
            "bool": "BOOLEAN"
        }

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
        cursor = self.connection.cursor()
        try:
            cursor.execute(create_table_query)
            self.connection.commit()
            print(f"table {table_name} created!!")
            # self.insert_data_from_df(df, table_name)
        except Exception as e:
            print(f"Error creating table. {e}")
        finally:
            cursor.close()

    def insert_data_from_df(self, df, table_name):
        if df.empty:
            print("Error: DataFrame is empty.")
            return

        cursor = self.connection.cursor()
        for i, row in df.iterrows():
            insert_query = f"""
            INSERT INTO `{table_name}` ({', '.join(df.columns)})
            VALUES ({', '.join(['%s'] * len(row))});
            """
            try:
                cursor.execute(insert_query, tuple(row))
                self.connection.commit()
            except Exception as e:
                print(f"Error inserting data: {e}")
                self.connection.rollback()  # Rollback if there's an error
        print("data inserted!")
        cursor.close()

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Closed db connection!")
            logging.info("Closed database connection!")


if __name__ == "__main__":
    # Sample DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Pudding"],
        "salary": [1000.5, 1500.75, 2000.0, 2012.3],
        "is_active": [True, False, True, False],
        "hire_date": pd.to_datetime(["2020-01-01", "2019-05-15", "2018-07-30", "2021-02-10"])
    })

    table_name = "Employee"
    manager = SQLManager()
    if (manager.create_table_from_df(df, table_name)):
        print("Successfully create table")
