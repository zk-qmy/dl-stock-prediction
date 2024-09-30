from sqlalchemy import create_engine, exc
import yaml
import mysql.connector


def load_config():
    try:
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print("Error config.yaml not found")
        exit(1)
    except yaml.YAMLError as e:
        print(f"Error reading file: {e}")
        exit(1)


def get_db_engine(config):
    # Setup DB connection
    db_config = config['database']
    try:
        engine = create_engine(f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
        return engine
    except exc.SQLAlchemyError as e:
        print(f"Database Connecting failed: {e}")
        exit(1)


def connect_to_db(config):
    mysql.connector.connect(
        user = config['database']['username'],
        password =config['database']['password'],
        host= config['database']['host'],
        database= config['database']['dbName'],
        ssl_disablrd=True
        )