import yaml
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, exc, text, inspect
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
                logging.error(f"Database Connecting failed: {e}")
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

    def check_table_exists(self, conn, table_name):
        table_name = table_name.lower()
        query = f"SHOW TABLES LIKE '{table_name}'"
        try:
            result = conn.execute(text(query)).fetchone()
            if result is None:
                return False
            return True
        except Exception as e:
            raise CustomException(
                f"Error checking `{table_name}` existence: {e}", sys)

    def check_empty_table(self, conn, table_name):
        table_name = table_name.lower()
        query = text(f"SELECT COUNT(*) FROM {table_name}")
        try:
            table_row_count = conn.execute(query).fetchone()[0]
            return table_row_count
        except Exception as e:
            logging.error(f"Error checking `{table_name}` emptiness: {e}")
            raise CustomException(e, sys)

    def truncate_table(self, conn, table_name):
        table_name = table_name.lower()
        if not self.check_table_exists(conn, table_name):
            logging.error("Table does not exist to truncate")
            raise CustomException("Table does not exist to truncate")
        query = text(f"TRUNCATE TABLE {table_name}")
        try:
            conn.execute(query)
            logging.info(f"Table `{table_name}` truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncate table `{table_name}")
            raise CustomException(e, sys)

    def create_table_like(self, conn, output_table, source_table):
        output_table = output_table.lower()
        source_table = source_table.lower()
        # check if source_table exist
        if not self.check_table_exists(conn, source_table):
            raise CustomException(
                f"Source table `{source_table}` does not exist. Create one!")
        # check if output table already exist
        if self.check_table_exists(conn, output_table):
            logging.warning(f"Table `{output_table}` already exist.")
            return
        try:
            query = f"CREATE TABLE {output_table} LIKE {source_table};"
            conn.execute(text(query))
            logging.info(
                f"Table `{output_table}` created from `{source_table}`.")
        except Exception as e:
            raise CustomException(
                f"Error creating `{output_table}` from {source_table}: {e}", sys)

# Metadata table: Insert metadata
    def create_metadata_table(self, meta_table_name):
        table_name = meta_table_name.lower()

        query = f"""CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            last_crawl_date DATE
            );
            """
        # Execute query
        with self.connection.connect() as conn:
            try:
                if self.check_table_exists(conn, table_name):
                    logging.info(f"Table `{table_name}` already exists.")
                    return
                conn.execute(text(query))
                logging.info(f"Table `{table_name}` created!")
                return
            except Exception as e:
                raise CustomException(
                    f"Error creating table `{table_name}`: {e}", sys)

    def update_metadata_table(self, conn, last_crawl_date,
                              meta_table_name):
        table_name = meta_table_name.lower()
        # Check if table exist
        if not self.check_table_exists(conn, table_name):
            raise CustomException(f"Table `{table_name}` not exist.")

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
                last_crawl_date = datetime.strptime(
                    last_crawl_date, "%Y-%m-%d")
                logging.info(f"After str conversion:{last_crawl_date} ")
            except ValueError as e:
                logging.error(f"Invalid date format {last_crawl_date}: {e}")
                raise CustomException(f"Invalid date format: {e}", sys)
        elif isinstance(last_crawl_date, datetime):
            # If it's already a datetime object, ensure it's correct format
            last_crawl_date = last_crawl_date.strftime("%Y-%m-%d")
            logging.info(f"After object conversion: {last_crawl_date}")
        else:
            raise CustomException(
                f"Invalid type for last_crawl_date: {type(last_crawl_date)}")

        if (last_crawl_date is None):
            raise CustomException("last_crawl_date is `None`")
        # Create query
        query = f"""INSERT INTO {table_name} (last_crawl_date)
            VALUES (:last_crawl_date);
            """
        # Execute query
        # with self.connection.connect() as conn:
        try:
            # Begin a transaction
            # trans = conn.begin()
            result = conn.execute(text(query),
                                  {'last_crawl_date': last_crawl_date})
            if result.rowcount > 0:
                # trans.commit()
                logging.info(
                    f"Successfully inserted last_crawl_date into `{table_name}`")
            else:
                logging.warning(f"No rows inserted into `{table_name}`.")
        except Exception as e:
            logging.error(f"Error updating table `{table_name}`: {e}")
            # trans.rollback()  # Rollback in case of error
            raise CustomException(e, sys)

    def fetch_last_crawl_date(self, meta_table_name):
        table_name = meta_table_name.lower()
        query = f"""SELECT last_crawl_date FROM {table_name}
            ORDER BY last_crawl_date DESC
            LIMIT 1"""
        # Execute query
        with self.connection.connect() as conn:
            try:
                if not self.check_table_exists(conn, table_name):
                    raise CustomException(f"Create {table_name} before fetching!")
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
                raise CustomException(
                    f"Error retrieving from `{table_name}`: {e}", sys)

# Tables
    def create_table_from_df(self, conn, df, table_name):
        if self.check_table_exists(conn, table_name):
            logging.info(f"Table `{table_name}` already exists.")
            return

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
            raise CustomException("Error: DataFrame is empty.")

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
        # with self.connection.connect() as conn:
        try:
            conn.execute(text(create_table_query))
            logging.info("Start creating table `{table_name}` in db")
            logging.info(f"Generated query:\n{create_table_query}")
            logging.info(f"Table `{table_name}` created!")
            return
        except Exception as e:
            raise CustomException(
                f"Error creating table `{table_name}`: {e}", sys)

# Staging table: Insert raw data to staging table
    def insert_data_from_df(self, conn, df, table_name, chunksize=1000):
        table_name = table_name.lower()
        if df.empty:
            raise CustomException("Error: DataFrame is empty.")
        # with self.connection.connect() as conn:
        try:
            # Begin a transaction
            # trans = conn.begin()
            # Use SQLAlchemy's `to_sql` for bulk insert
            df.to_sql(
                table_name,
                conn,  # self.connection,
                if_exists="append",
                index=False,
                chunksize=chunksize)
            # trans.commit()
            logging.info(
                f"Data inserted into `{table_name}` successfully!")
            return
        except Exception as e:
            # trans.rollback()
            raise CustomException(
                f"Error inserting data into `{table_name}`: {e}", sys)

# Main table: upsert data from Staging table to main table
    def upsert_data_from_table_to_table(self, conn,
                                        input_table_name,
                                        destination_table_name):
        input_table_name = input_table_name.lower()
        destination_table_name = destination_table_name.lower()
        # check if tables exist
        # (tb_frame error)
        if not self.check_table_exists(conn, input_table_name):
            raise CustomException(
                f"Table `{input_table_name}` does not exist. Create it first!")
        if not self.check_table_exists(conn, destination_table_name):
            raise CustomException(
                f"Table `{destination_table_name}` does not exist. Create it first!")
        # check if tables empty
        input_table_rows = self.check_empty_table(conn, input_table_name)
        if input_table_rows < 1:
            logging.warning(f"Warning input table {input_table_name}"
                            + f" has {input_table_rows} rows. Skip operation!")
            return

        # Get column names from the destination table
        try:
            inspector = inspect(conn)
            columns = [column['name']
                       for column in inspector.get_columns(destination_table_name)]

            if not columns:
                raise CustomException(
                    f"No columns found in destination table `{destination_table_name}`.")
            # Generate the insert column names and VALUES() for the query
            column_names = ', '.join(columns)
            logging.info(f"Column names: {column_names}")
            # Construct the SQL query with ON DUPLICATE KEY UPDATE
            query = text(f"""
                INSERT INTO {destination_table_name} ({column_names})
                SELECT {', '.join(columns)} FROM {input_table_name} AS source
                ON DUPLICATE KEY UPDATE
                    {', '.join([f"{col} = source.{col}" for col in columns])};
            """)

            logging.info(f"`{input_table_name}` to `{destination_table_name}` upsert query: \n{query}")
            # Execute the query
            # with conn.begin():
            conn.execute(query)
            logging.info(f"{input_table_rows} data upserted"
                         + f" from `{input_table_name}`"
                         + f" to `{destination_table_name}` successfully!")

        except CustomException as ce:
            logging.error(f"Custom exception: {ce}")
            raise ce
        except Exception as e:
            logging.error(f"Error upsert data from `{input_table_name}`"
                          + f" to `{destination_table_name}`")
            raise CustomException(f"Failed to upsert data: {str(e)}", sys)

# Backup table: create backup + upsert using the same method as staging to main

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Closed db connection!")
            logging.info("Closed database connection!")

# Groups of operation
    def run_group1_insert_raw_to_staging(self, df, table_name):
        # Group 1: insert raw data to staging
        with self.connection.connect() as conn:
            try:
                # Begin a global transaction
                trans = conn.begin()

                # Operation 1: Create table from df
                self.create_table_from_df(conn, df, table_name)
                # logging.info(f"Done creating table `{table_name}`!")
                # Operation 2: Insert data from df to table
                self.insert_data_from_df(conn, df, table_name)
                # logging.info("Done inserting raw data to db!")

                # If all operations succeed, commit the global transaction
                trans.commit()
            except Exception as e:
                logging.error(f"Error in group 1: {e}")
                # Global rollback in case of any failure in the entire sequence
                trans.rollback()
                raise CustomException(e, sys)

    def run_group2_insert_staging_to_main_n_backup(self, staging_table,
                                                   main_table,
                                                   backup_table):
        # Group 2: insert new data from staging to main + backup
        with self.connection.connect() as conn:
            try:
                # Begin a global transaction
                trans = conn.begin()

                # Operation 1: upsert staging data to main table
                self.upsert_data_from_table_to_table(conn, staging_table,
                                                     main_table)
                # logging.info(f"Done upsert to `{main_table}`!")
                # Operation 2: upsert staging data to backup table
                self.upsert_data_from_table_to_table(conn, staging_table,
                                                     backup_table)
                # logging.info(f"Done upsert to `{backup_table}`!")

                # If all operations succeed, commit the global transaction
                trans.commit()
            except Exception as e:
                logging.error(f"Error in group 2: {e}")
                # Global rollback in case of any failure in the entire sequence
                trans.rollback()
                raise CustomException(e, sys)

    def run_group3_update_meta_n_clear_staging(self, last_crawl_date,
                                               meta_table_name,
                                               staging_table_name):
        # NOTICE: CREATE META TABLE BEFORE RUN THIS
        # Group 3: update meta table + clear staging
        with self.connection.connect() as conn:
            try:
                # Begin a global transaction
                trans = conn.begin()

                # Operation 1: Update meta table
                self.update_metadata_table(conn, last_crawl_date,
                                           meta_table_name)
                # Operation 2: clear staging table
                self.truncate_table(conn, staging_table_name)

                # If all operations succeed, commit the global transaction
                trans.commit()
            except Exception as e:
                logging.error(f"Error in group 3: {e}")
                # Global rollback in case of any failure in the entire sequence
                trans.rollback()
                raise CustomException(e, sys)


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

    # New DataFrame for upsert
    df_new = pd.DataFrame({
        "id": [6],  # Note: '3' and '4' exist in the original, '6' is new
        "name": ["New Employee"],
        "salary": [3000.0],
        "is_active": [True],
        "hire_date": pd.to_datetime([
            "2023-11-15"
        ])
    })

    staging_table_name = "staging_employees"
    main_table_name = "main_employees"
    backup_table_name = "backup_employees"
    meta_table = "meta"
    manager = SQLManager()
    try:
        manager.run_group1_insert_raw_to_staging(df, staging_table_name)
        print("Done group 1")
        with manager.connection.connect() as connection:
            manager.create_table_like(
                connection, main_table_name, staging_table_name)
            manager.create_table_like(
                connection, backup_table_name, main_table_name)
            manager.create_metadata_table(connection, meta_table)
        manager.run_group2_insert_staging_to_main_n_backup(
            staging_table_name, main_table_name, backup_table_name)
        print("Done group 2")
        manager.run_group3_update_meta_n_clear_staging(
            "2021-12-03", meta_table_name=meta_table,
            staging_table_name=staging_table_name)
        print("Done processing!!!!!!")
    except CustomException as e:
        logging.error(e)
    # TO DO: handle connection more effective
    # TO DO: fix this bug? when insert the data.
    # if the data of the next time is the same,
    # those data will still be inserted
