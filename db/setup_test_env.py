from database import Database
from configparser import ConfigParser

config = ConfigParser()
config.read('../config.ini')

#LASTFM_API_KEY = config['LASTFM']['api_key']
DATABASE_HOST = config['MYSQL']['db_path']
DATABASE_USER = config['MYSQL']['db_user']
DATABASE_PASSWORD = config['MYSQL']['db_pwd']
DATABASE_DB = config['MYSQL']['db_database']

if __name__ == '__main__':
    cxn = Database(DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, "sandbox")
    cxn.connect()
    cxn.drop_table("tags")
    cxn.drop_table("genres")
    cxn.drop_table("history")
    cxn.drop_table("track_data")
    cxn.drop_table("similar_artists")
    cxn.drop_table("artists")
    cxn.drop_table("track_genres")
    cxn.close()