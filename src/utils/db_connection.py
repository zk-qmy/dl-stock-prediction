from sqlalchemy import create_engine
import yaml


def get_db_engine():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    db_config = config['database']
    return create_engine(f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
