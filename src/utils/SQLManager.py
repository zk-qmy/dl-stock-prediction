from src.utils.db_connection import SQLConnection

import pandas as pd


class SQLManager:
    def __init__(self):
        self.connection = SQLConnection().get_db_connection()

    def create_table_from_df(self, df, table_name):
        mapping = {
            "int64": "BIGINT",
            "float64": "FLOAT",
            "object": "VARCHAR(10)",
            "datetime64[ns]": "DATE",
            "bool": "BOOLEAN"
        }
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
            self.insert_data_from_df(df, table_name)
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


if __name__ == "__main__":
    # Sample DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "salary": [1000.5, 1500.75, 2000.0],
        "is_active": [True, False, True],
        "hire_date": pd.to_datetime(["2020-01-01", "2019-05-15", "2018-07-30"])
    })

    table_name = "employees"
    manager = SQLManager()
    if (manager.create_table_from_df(df, table_name)):
        print("Successfully create table")
