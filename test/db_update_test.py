# Description: This script is used to test the database update functions.
# It duplicates the functionality of db_test.py, but with the addition update functions.
# We run this after we are satisfied with the results of db_test.py.

import db.db_functions as dbf
import db.db_update as dbu
import analysis.lastfm as lfm
from time import sleep
from config import setup_logging

setup_logging("logs/db_update_test.log")

database = dbu.database
database.connect()

#drop tables
database.drop_all_tables()

# Create tables
database.create_all_tables()


# Populate test db
dbf.insert_tracks(dbf.database, 'output/test_track_data2.csv')
# after this export, we can run bpm analysis on a different machine
dbf.populate_artists_table(dbf.database)
dbf.populate_artist_id_column(dbf.database)

# db_update functions to populate the genres table and the track_genre table
genre_list = dbu.populate_genres_table_from_track_data(database)
dbu.insert_genres_if_not_exists(database, genre_list)
dbu.populate_track_genre_table(database)

# Get last.fm artist data
# artists = dbu.get_artists_from_db(database)
dbu.insert_last_fm_artist_data(database)  # Pass list once

# Get last.fm track data
tracks = lfm.get_track_list_from_db(database)
for track in tracks:
    sleep(2)
    dbu.insert_lastfm_track_data(database, track)
