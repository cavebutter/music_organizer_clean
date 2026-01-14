from . import DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT, TEST_DB
from .database import Database
import analysis.acousticbrainz as acousticbrainz
from loguru import logger
import csv
import json
import re
from time import sleep
import analysis.lastfm as lastfm
import pdb

# TODO change database for production
database = Database(DB_PATH, DB_USER, DB_PASSWORD, TEST_DB)


def process_bpm(database: Database, track_list: csv, location_prefix: str):
    """
    Process the BPM for each track in the track list and update the 'bpm' field in the database.


    Parameters:
    database (Database): The database connection object.
    track_list (csv): The list of tracks to process BPM for.
    location_prefix (str): The prefix to prepend to the file location. Must include trailing slash.

    Returns:
    None
    """
    database.connect()
    with open(track_list, 'r') as f:
        reader = csv.DictReader(f)
        lib_size = sum(1 for _ in reader)
        logger.debug(f"Library size: {lib_size}")


    with open(track_list, 'r') as f:
        reader = csv.DictReader(f)
        i = 1
        for row in reader:
            file_location = location_prefix + row['location']
            # Above only applies to Neptune server. Change as needed.
            track_bpm = bpm.get_bpm(file_location)
            database.execute_query("UPDATE track_data SET bpm = %s WHERE id = %s", (track_bpm, row['id']))
            logger.info(f"Processed BPM for {row['plex_id']}; {i} of {lib_size}")
            i += 1
    database.close()


def populate_genres_table_from_track_data(database: Database):
    logger.debug("Starting to populate genres table from track data.")
    database.connect()
    query = "SELECT genre FROM track_data"
    results = database.execute_select_query(query)
    genre_list = []

    for result in results:
        genre_str = result[0]
        if genre_str != '[]':
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                genre_list.extend(genres)
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    genre_list = list(set(genre_list))
    logger.info(f"Extracted genres: {genre_list}")
    database.close()
    logger.debug("Finished populating genres table from track data.")
    return genre_list


def insert_genres_if_not_exists(database: Database, genre_list: list):
    logger.debug("Starting to insert genres if not exists.")
    database.connect()

    existing_genres_query = "SELECT genre FROM genres"
    existing_genres = database.execute_select_query(existing_genres_query)
    existing_genres_set = {genre[0] for genre in existing_genres}

    new_genres = [genre for genre in genre_list if genre not in existing_genres_set]

    for genre in new_genres:
        database.execute_query("INSERT INTO genres (genre) VALUES (%s)", (genre,))
        logger.info(f"Inserted new genre: {genre}")

    database.close()
    logger.debug("Finished inserting genres if not exists.")
    return new_genres


def populate_track_genre_table(database: Database):
    logger.debug("Starting to populate track genre table.")
    database.connect()
    query = "SELECT id, genre FROM track_data"
    results = database.execute_select_query(query)

    for result in results:
        track_id = result[0]
        genre_str = result[1]
        if genre_str != '[]':
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                for genre in genres:
                    genre_id_query = "SELECT id FROM genres WHERE genre = %s"
                    genre_id_result = database.execute_select_query(genre_id_query, (genre,))
                    if genre_id_result:
                        genre_id = genre_id_result[0][0]
                        database.execute_query("INSERT INTO track_genres (track_id, genre_id) VALUES (%s, %s)", (track_id, genre_id))
                        logger.info(f"Inserted track-genre pair: track_id={track_id}, genre_id={genre_id}")
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    database.close()
    logger.debug("Finished populating track genre table.")
    return None


def update_track_genre_table(database: Database, cutoff: str = None):
    logger.debug("Starting to update track genre table.")
    database.connect()
    query_wo_cutoff = "SELECT id, genre FROM track_data"
    query_w_cutoff = "SELECT id, genre FROM track_data WHERE added_date > %s"

    if cutoff is None:
        results = database.execute_select_query(query_wo_cutoff)
    else:
        try:
            cutoff_date = re.sub(r'(\d{2})(\d{2})(\d{4})', r'\3-\1-\2', cutoff)
            results = database.execute_select_query(query_w_cutoff, (cutoff_date,))
        except Exception as e:
            logger.error(f"There was an error querying db with cutoff: {e}")
            results = []

    for result in results:
        track_id = result[0]
        genre_str = result[1]
        if genre_str != '[]':
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                for genre in genres:
                    genre_id_query = "SELECT id FROM genres WHERE genre = %s"
                    genre_id_result = database.execute_select_query(genre_id_query, (genre,))
                    if genre_id_result:
                        genre_id = genre_id_result[0][0]
                        database.execute_query("INSERT INTO track_genres (track_id, genre_id) VALUES (%s, %s)", (track_id, genre_id))
                        logger.info(f"Inserted track-genre pair: track_id={track_id}, genre_id={genre_id}")
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    database.close()
    logger.debug("Finished updating track genre table.")
    return None


def get_artists_from_db(database: Database):
    """
    Get all artists from artists table in the database. Return a list of artist names.
    :param database:
    :return:
    """
    logger.debug("Starting to get artists from db.")
    database.connect()
    query = "SELECT artist FROM artists"
    results = database.execute_select_query(query)
    artist_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting artists from db.")
    return artist_list


def check_mbid_and_insert(database: Database, lastfm_json: json, mbid_list: list):
    """
    Check if the MBID is in the database and insert it in artists.musicbrainz_id if it is not.
    :param database:
    :param lastfm_json:
    :param mbid_list:
    :return:
    """
    database.connect()
    mbid = lastfm.get_artist_mbid(lastfm_json)
    if mbid not in mbid_list:
        artist = lastfm_json['artist']['name']
        database.execute_query("UPDATE artists SET musicbrainz_id = %s WHERE artist = %s", (mbid, artist))
        logger.info(f"Inserted MBID for {artist}: {mbid}")


def check_tags_and_insert(database: Database, lastfm_json: json, genre_list: list):
    database.connect()
    tags = lastfm.get_artist_tags(lastfm_json)
    for tag in tags:
        if tag.lower() not in [g.lower() for g in genre_list]:  # Case-insensitive check
            database.execute_query("INSERT INTO genres (genre) VALUES (%s)", (tag,))
            logger.info(f"Inserted new genre: {tag}")
            genre_list.append(tag)  # Add to list to prevent duplicates
    database.close()


def insert_last_fm_artist_data(database: Database):
    logger.debug("Starting to insert Last.fm data into db.")
    database.connect()

    try:
        artists = database.execute_select_query("SELECT id, artist FROM artists")

        for artist_id, artist_name in artists:
            try:
                sleep(1)  # Rate limiting
                artist_info = lastfm.get_artist_info(artist_name)
                if not artist_info:
                    logger.error(f"Failed to retrieve artist info for {artist_name}")
                    continue

                # Update MusicBrainz ID if available
                mbid = lastfm.get_artist_mbid(artist_info)
                if mbid:
                    logger.debug(f"MBID for {artist_name}: {mbid}")
                    database.execute_query(
                        "UPDATE artists SET musicbrainz_id = %s WHERE id = %s",
                        (mbid, artist_id)
                    )

                # Process genres
                genres = lastfm.get_artist_tags(artist_info)
                for genre in genres:
                    genre = genre.lower()
                    try:
                        # Insert genre if not exists using WHERE NOT EXISTS
                        database.execute_query("""
                            INSERT INTO genres (genre)
                            SELECT %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM genres 
                                WHERE LOWER(genre) = LOWER(%s)
                            )
                        """, (genre, genre))

                        # Get genre ID
                        genre_id = database.execute_select_query(
                            "SELECT id FROM genres WHERE LOWER(genre) = LOWER(%s)",
                            (genre,)
                        )[0][0]

                        # Insert genre relationship if not exists
                        database.execute_query("""
                            INSERT INTO artist_genres (artist_id, genre_id)
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM artist_genres
                                WHERE artist_id = %s AND genre_id = %s
                            )
                        """, (artist_id, genre_id, artist_id, genre_id))

                        logger.info(f"Processed genre for {artist_name}: {genre}")
                    except Exception as e:
                        logger.error(f"Error processing genre {genre} for {artist_name}: {e}")
                        continue

                # Process similar artists
                similar_artists = lastfm.get_similar_artists(artist_info)
                logger.debug(f"Similar artists for {artist_name}: {similar_artists}")

                for similar_artist in similar_artists:
                    if not similar_artist:
                        continue

                    try:
                        # Insert similar artist if not exists
                        database.execute_query("""
                            INSERT INTO artists (artist)
                            SELECT %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM artists 
                                WHERE LOWER(artist) = LOWER(%s)
                            )
                        """, (similar_artist, similar_artist))

                        # Get similar artist ID
                        similar_artist_id = database.execute_select_query(
                            "SELECT id FROM artists WHERE LOWER(artist) = LOWER(%s)",
                            (similar_artist,)
                        )[0][0]

                        # Insert similar artist relationship if not exists
                        database.execute_query("""
                            INSERT INTO similar_artists (artist_id, similar_artist_id)
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM similar_artists
                                WHERE artist_id = %s AND similar_artist_id = %s
                            )
                        """, (artist_id, similar_artist_id, artist_id, similar_artist_id))

                        logger.info(f"Processed similar artist: {artist_name} -> {similar_artist}")
                    except Exception as e:
                        logger.error(f"Error processing similar artist {similar_artist} for {artist_name}: {e}")
                        continue

            except Exception as e:
                logger.error(f"Error processing artist {artist_name}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in Last.fm data insertion process: {e}")
        raise e
    finally:
        database.close()

    logger.debug("Finished inserting Last.fm data into db.")


def insert_lastfm_track_data(database: Database, db_track_data: tuple[int, str, str]):
    """
    Take db track data (id, artist name, track name) and retrieve lastfm data for each track.
    Insert mbid into track_data table.
    Insert genres into track_genres table.  If genre does not exist, insert into genres table, then create relationship.
    Args:
        database:
        db_track_data:

    Returns:

    """
    database.connect()
    try:
        lfm_track_data = lastfm.get_last_fm_track_data(db_track_data[1], db_track_data[2])
        if lfm_track_data:
            logger.debug(f"Received Last.fm data for {db_track_data[2]}: {lfm_track_data}")
            track_mbid = lastfm.get_track_mbid(lfm_track_data)
            if track_mbid:
                database.execute_query("UPDATE track_data SET musicbrainz_id = %s WHERE id = %s", (track_mbid, db_track_data[0]))
                logger.info(f"Inserted MBID for {db_track_data[2]}: {track_mbid}")

                track_genres = lastfm.get_track_tags(lfm_track_data)
                # Insert genres if not exists
                for genre in track_genres:
                    genre = genre.lower()
                    try:
                        # Insert genre if not exists using WHERE NOT EXISTS
                        database.execute_query("""
                            INSERT INTO genres (genre)
                            SELECT %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM genres 
                                WHERE LOWER(genre) = LOWER(%s)
                            )
                        """, (genre, genre))

                        # Get genre ID
                        genre_id = database.execute_select_query(
                            "SELECT id FROM genres WHERE LOWER(genre) = LOWER(%s)",
                            (genre,)
                        )[0][0]

                        # Insert genre relationship if not exists
                        database.execute_query("""
                            INSERT INTO track_genres (track_id, genre_id)
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM track_genres
                                WHERE track_id = %s AND genre_id = %s
                            )
                        """, (db_track_data[0], genre_id, db_track_data[0], genre_id))

                        logger.info(f"Processed genre for {db_track_data[2]}: {genre}")
                    except Exception as e:
                        logger.error(f"Error processing genre {genre} for {db_track_data[2]}: {e}")
    except Exception as e:
        logger.error(f"Error processing track {db_track_data[2]}: {e}")


def process_bpm_acousticbrainz(database: Database) -> dict:
    """
    Fetch BPM from AcousticBrainz for tracks that have a MusicBrainz ID but no BPM.

    This queries the database for tracks with musicbrainz_id but null/zero bpm,
    looks up BPM via the AcousticBrainz API, and updates the database.

    Args:
        database: Database connection object

    Returns:
        dict with stats: {'total': int, 'hits': int, 'misses': int, 'updated': int}
    """
    logger.info("Starting AcousticBrainz BPM lookup")
    database.connect()

    # Get tracks with MBID but no BPM
    query = """
        SELECT id, musicbrainz_id
        FROM track_data
        WHERE musicbrainz_id IS NOT NULL
          AND musicbrainz_id != ''
          AND (bpm IS NULL OR bpm = 0)
    """
    tracks = database.execute_select_query(query)
    total = len(tracks)

    if total == 0:
        logger.info("No tracks found needing BPM lookup")
        database.close()
        return {'total': 0, 'hits': 0, 'misses': 0, 'updated': 0}

    logger.info(f"Found {total} tracks with MBID but no BPM")

    # Fetch BPMs from AcousticBrainz
    bpm_results = acousticbrainz.fetch_bpm_for_tracks(tracks, use_bulk=True)

    # Update database with results
    updated = 0
    for track_id, bpm_value in bpm_results.items():
        try:
            # Round BPM to nearest integer for storage
            bpm_int = round(bpm_value)
            database.execute_query(
                "UPDATE track_data SET bpm = %s WHERE id = %s",
                (bpm_int, track_id)
            )
            updated += 1
            logger.debug(f"Updated track {track_id} with BPM {bpm_int}")
        except Exception as e:
            logger.error(f"Failed to update BPM for track {track_id}: {e}")

    database.close()

    stats = {
        'total': total,
        'hits': len(bpm_results),
        'misses': total - len(bpm_results),
        'updated': updated
    }

    logger.info(f"AcousticBrainz BPM lookup complete: {stats}")
    logger.info(f"Hit rate: {stats['hits']/total*100:.1f}%")

    return stats

