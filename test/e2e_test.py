import plex.plex_library as p
import db.db_functions as dbf
import db.db_update as dbu
import analysis.lastfm as lfm
from time import sleep
from config import setup_logging


if __name__ == '__main__':
    setup_logging("logs/e2e.log")

    db = dbu.database
    db.connect()

    # drop tables
    db.drop_all_tables()

    # Create tables
    db.create_all_tables()

    # Connect to Plex server and get music library
    server = p.plex_connect()  # Connect to TEST_SERVER
    library = p.get_music_library(server, p.TEST_LIBRARY) # Get TEST_LIBRARY
    tracks, lib_size = p.get_all_tracks_limit(library)
    track_list = p.listify_track_data(tracks,'/mnt/hdd/')
    p.export_track_data(track_list, 'output/e2e_test_track_data.csv')



    # Populate test db
    dbf.insert_tracks(dbf.database, 'output/e2e_test_track_data.csv')
    # after this export, we can run bpm analysis on a different machine
    dbf.populate_artists_table(dbf.database)
    dbf.populate_artist_id_column(dbf.database)

    # db_update functions to populate the genres table and the track_genre table
    genre_list = dbu.populate_genres_table_from_track_data(db)
    dbu.insert_genres_if_not_exists(db, genre_list)
    dbu.populate_track_genre_table(db)

    # Get last.fm artist data
    # artists = dbu.get_artists_from_db(database)
    dbu.insert_last_fm_artist_data(db)  # Pass list once

    # Get last.fm track data
    tracks = lfm.get_track_list_from_db(db)
    for track in tracks:
        sleep(.5)
        dbu.insert_lastfm_track_data(db, track)

    # get bpm data
    ids_locations = dbf.get_id_location(db)
    dbf.export_results(ids_locations, 'output/e2e_test_id_location.csv')
    dbu.process_bpm(db, 'output/e2e_test_id_location.csv', "/mnt/hdd/")

    # Update history
    dbf.update_history(db, lib_size)