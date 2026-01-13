import configparser

config = configparser.ConfigParser()

config.read('config.ini')

DB_PATH = config['MYSQL']['db_path']
DB_USER = config['MYSQL']['db_user']
DB_PASSWORD = config['MYSQL']['db_pwd']
DB_DATABASE = config['MYSQL']['db_database']
TEST_DB = config['MYSQL_TEST']['db_database']
DB_PORT = config['MYSQL']['db_port']

__all__ = [
    'DB_PATH',
    'DB_USER',
    'DB_PASSWORD',
    'DB_DATABASE',
    'TEST_DB',
    'DB_PORT'
]