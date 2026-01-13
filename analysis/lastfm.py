from configparser import ConfigParser
import requests
import json
from loguru import logger
from db.database import Database
from db import DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE, TEST_DB

config = ConfigParser()
config.read('config.ini')

LASTFM_API_KEY = config['LASTFM']['api_key']
LASTFM_SHARED_SECRET = config['LASTFM']['shared_secret']
LASTFM_USERNAME = config['LASTFM']['username']
LASTFM_PASSWORD = config['LASTFM']['password']
LASTFM_APP_NAME = config['LASTFM']['app_name']

database = Database(DB_PATH, DB_USER, DB_PASSWORD, TEST_DB) # Change to production db
def get_artist_info(artist_name):

    """
    Retrieves information about a specific artist from the Last.fm API.

    Parameters:
    artist_name (str): The name of the artist to retrieve information for.

    Returns:
    dict: A JSON object containing information about the artist if the request is successful, otherwise None.
    """
    url = f'http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&autocorrect=1&artist={artist_name}&api_key={LASTFM_API_KEY}&format=json'
    response = requests.get(url)
    if response.status_code == 200:
        logger.debug(f"last_fm Response: {response.json()}")
        logger.info(f"Retrieved artist info for {artist_name}")
        return response.json()
    else:
        logger.error(f"Failed to retrieve artist info for {artist_name}")
        return None


def get_artist_mbid(result: json):
    """
    Retrieves the MusicBrainz ID (MBID) of the artist from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing artist information.

    Returns:
    str: The MusicBrainz ID (MBID) of the artist, or None if the MBID is not found.
    """
    try:
        mbid = result['artist']['mbid']
        logger.info(f"Retrieved MBID for {result['artist']['name']}: {mbid}")
        return mbid
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve MBID for {result['artist']['name']}: {e}")
        return None


def get_artist_tags(result: json):
    """
    Retrieves the tags from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing artist information.

    Returns:
    list: A list of tags associated with the artist, or an empty list if no tags are found.
    """
    try:
        tags = result['artist']['tags']['tag']
        tag_list = [tag['name'] for tag in tags]
        logger.info(f"Retrieved tags for {result['artist']['name']}: {tag_list}")
        return tag_list
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve tags for {result['artist']['name']}: {e}")
        return []


def get_similar_artists(result):
    """Extract similar artists from Last.fm API response.

    Args:
        result (dict): Last.fm API response containing artist info

    Returns:
        list: List of similar artist names
    """
    try:
        similar_artists = []
        if 'artist' in result and 'similar' in result['artist']:
            for artist in result['artist']['similar']['artist']:
                similar_artists.append(artist['name'])
        return similar_artists

    except Exception as e:
        logger.error(f"Failed to retrieve similar artists for {result['artist']['name']}: {e}")
        return []
    # try:
    #     similar_artists = result.get('similar', {}).get('artist', [])
    #     return [artist.get('name') for artist in similar_artists]
    # except (KeyError, AttributeError):
    #     return []

def get_current_mbids_from_db(database: Database):
    """
    Get all MusicBrainz IDs (MBIDs) from the artists table in the database.

    Parameters:
    database (Database): The database object to query.

    Returns:
    list: A list of MusicBrainz IDs (MBIDs) from the database.
    """
    logger.debug("Starting to get MBIDs from db.")
    database.connect()
    query = "SELECT musicbrainz_id FROM artists"
    results = database.execute_select_query(query)
    mbid_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting MBIDs from db.")
    return mbid_list


def get_genres_from_db(database: Database):
    """
    Get all genres from the genres table in the database.

    Parameters:
    database (Database): The database object to query.

    Returns:
    list: A list of genres from the database.
    """
    logger.debug("Starting to get genres from db.")
    database.connect()
    query = "SELECT genre FROM genres"
    results = database.execute_select_query(query)
    genre_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting genres from db.")
    return genre_list


def get_last_fm_track_data(artist: str, track: str):
    # NOTE this does not cover cases where an artist records multiple tracks with the same name.
    # Last.fm API only returns a single artist/track pair.
    """
    Retrieves information about a specific track from the Last.fm API.

    Parameters:
    artist (str): The name of the artist.
    track (str): The name of the track.

    Returns:
    dict: A JSON object containing information about the track if the request is successful, otherwise None.
    """
    url = f'http://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={LASTFM_API_KEY}&artist={artist}&track={track}&autocorrect=1&format=json'
    response = requests.get(url)
    if response.status_code == 200:
        logger.debug(f"last_fm Response: {response.json()}")
        logger.info(f"Retrieved track info for {artist} - {track}")
        return response.json()
    else:
        logger.error(f"Failed to retrieve track info for {artist} - {track}")
        return None



def get_track_mbid(result: json):
    """
    Retrieves the MusicBrainz ID (MBID) of the track from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing track information.

    Returns:
    str: The MusicBrainz ID (MBID) of the track, or None if the MBID is not found.
    """
    try:
        mbid = result['track']['mbid']
        logger.info(f"Retrieved MBID for {result['track']['name']}: {mbid}")
        return mbid
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve MBID for {result['track']['name']}: {e}")
        return None


def get_track_tags(result: json):
    """
    Retrieves the tags from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing track information.

    Returns:
    list: A list of tags associated with the track, or an empty list if no tags are found.
    """
    try:
        tags = result['track']['toptags']['tag']
        tag_list = [tag['name'] for tag in tags]
        logger.info(f"Retrieved tags for {result['track']['name']}: {tag_list}")
        return tag_list
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve tags for {result['track']['name']}: {e}")
        return []


def get_track_list_from_db(database: Database):
    """
    Get all tracks from the tracks table in the database.

    Parameters:
    database (Database): The database object to query.

    Returns:
    list: A list of tracks from the database.
    """
    logger.debug("Starting to get tracks from db.")
    database.connect()
    query = """SELECT td.id, a.artist, td.title
    FROM track_data td
    INNER JOIN artists a ON td.artist_id = a.id"""
    results = database.execute_select_query(query)
    track_list = [(result[0], result[1], result[2]) for result in results]
    database.close()
    logger.debug("Finished getting tracks from db.")
    return track_list