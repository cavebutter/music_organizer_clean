import subprocess
from db.database import Database
import db.db_functions as dbf


# Run setup_test_env
subprocess.run(['python3', 'db/setup_test_env.py'])

# Create tables
db_ddl = Database(dbf.DB_PATH, dbf.DB_USER, dbf.DB_PASSWORD, dbf.TEST_DB)
db_ddl.connect()
db_ddl.create_genres_table()
db_ddl.create_history_table()
db_ddl.create_artists_table()
db_ddl.create_tags_table()
db_ddl.create_similar_artists_table()
db_ddl.create_track_data_table()
db_ddl.close()

dbf.insert_tracks(dbf.database, 'output/test_track_data2.csv')
results = dbf.get_id_location(dbf.database)
dbf.export_results(results, 'output/test_id_location.csv')
# after this export, we can run bpm analysis on a different machine
dbf.populate_artists_table(dbf.database)
dbf.populate_artist_id_column(dbf.database)

