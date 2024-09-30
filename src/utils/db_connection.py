from sqlalchemy import create_engine, exc
import yaml
import mysql.connector
# pip install mysql-connector-python


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
        engine = create_engine(f"mysql+pymysql://"
                               f"{db_config['username']}:"
                               f"{db_config['password']}@"
                               f"{db_config['host']}:{db_config['port']}/"
                               f"{db_config['dbname']}")
        return engine
    except exc.SQLAlchemyError as e:
        print(f"Database Connecting failed: {e}")
        exit(1)


def connect_to_db(config):
    mysql.connector.connect(host=config['database']['host'],
                            port=config['database']['port'],
                            user=config['database']['username'],
                            password=config['database']['password'],
                            database=config['database']['dbname'],
                            timeout=None,
                            source_address=None)
