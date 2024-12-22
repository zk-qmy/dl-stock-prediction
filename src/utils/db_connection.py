from sqlalchemy import create_engine, exc
import yaml
import mysql.connector
# pip install mysql-connector-python


class SQLConnection:

    def __init__(self, config_path="config.yaml"):
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
        # Check if connection is already established
        if self.connection is not None:
            return self.connection

        try:
            self.connection = mysql.connector.connect(
                host=self.config['database']['host'],
                port=self.config['database']['port'],
                user=self.config['database']['username'],
                password=self.config['database']['password'],
                database=self.config['database']['dbname'],
                connection_timeout=None)
            print("database connected!")
            return self.connection
        except Exception as e:
            print(f"Error getting connection!Error: {e}")
            exit(1)
