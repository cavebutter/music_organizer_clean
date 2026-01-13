from . import WOODSTOCK_servername, musiclibrary, plex_password, plex_username, TEST_SERVER, TEST_LIBRARY
from plexapi.myplex import MyPlexAccount
from datetime import datetime
import sys
import csv
from loguru import logger

plex_username = plex_username
plex_password = plex_password
PLEX_SERVER = WOODSTOCK_servername
MUSIC_LIBRARY = musiclibrary
TEST_SERVER = TEST_SERVER
TEST_LIBRARY = TEST_LIBRARY

def plex_connect():
    """
    Connects to a Plex server using the credentials and server name from the configuration.

    Returns:
    PlexServer: The connected Plex server object.
    """
    account = MyPlexAccount(plex_username, plex_password)
    try:
        server = account.resource(TEST_SERVER).connect()
        logger.info(f"Connected to Plex Server: {TEST_SERVER}")
        return server
    except Exception as e:
        logger.error(f"Error connecting to Plex server {TEST_SERVER}: {e}")
        sys.exit()

def get_music_library(server, library):
    """
    Finds the music library in the provided Plex server.
    :param server: PLex Server object
    :param library: String representing the name of the music library
    :return: Music library object
    """
    try:
        music_library = server.library.section(library)
        logger.debug(f"Retrieved Plex server music library")
        return music_library
    except Exception as e:
        logger.error(f"Error retrieving music library: {e}")
        sys.exit()


def get_all_tracks(music_library):
    """
    Retrieve all tracks from the music library.
    :param music_library: Plex library object
    :return: list of track objects
    """
    try:
        tracks = music_library.searchTracks()
        library_size = len(tracks)
        logger.debug(f"Retrieved {library_size} tracks from Plex library")
        return tracks, library_size
    except Exception as e:
        logger.error(f"Error retrieving tracks from music library: {e}")
        sys.exit()

def get_all_tracks_limit(music_library, limit=50):
    """
    Retrieve all tracks from the music library.
    :param music_library: Plex library object
    :return: list of track objects
    """
    try:
        tracks = music_library.searchTracks(limit=limit)
        library_size = len(tracks)
        logger.debug(f"Retrieved {library_size} tracks from Plex library")
        return tracks, library_size
    except Exception as e:
        logger.error(f"Error retrieving tracks from music library: {e}")
        sys.exit()


def extract_track_data(track, filepath_prefix: str):
   #  TODO: This does not handle Various Artist albums well. Need to fix this.
    """
    Extract Plex track data from a track object. Return a dict with selected data
    along with a server_id for ratingKey and a stripped filepath.
    :param track: Plex track object
    :param filepath_prefix: string to be stripped from the location[0] field
    :return:
    """
    genre_list = []
    for genre in track.genres:
        genre_list.append(genre.tag)
    added_date = track.addedAt.strftime('%Y-%m-%d')
    filepath = None
    for media in track.media:
        for part in media.parts:
            filepath = part.file
    logger.debug(f"Original location: {track.locations[0]}")
    logger.debug(f"Filepath prefix: {filepath_prefix}")
    stripped_location = track.locations[0].replace(filepath_prefix, '')
    logger.debug(f"Stripped location: {stripped_location}")
    track_data = {
        'title': track.title,
        'artist': track.artist().title,
        'album': track.album().title,
        'genre': genre_list,
        'added_date': added_date,
        'filepath': filepath,
        'location': stripped_location,
        'plex_id': int(track.ratingKey)
    }
    return track_data


def listify_track_data(tracks, filepath_prefix: str):
    """
    Lists the track data from the provided list of tracks.

    Parameters:
    tracks (list): A list of track objects to extract data from.

    Returns:
    list: A list of dictionaries containing the track data.
    """
    track_list = []
    lib_size = len(tracks)
    i = 1
    for track in tracks:
        track_data = extract_track_data(track, filepath_prefix)
        track_list.append(track_data)
        logger.debug(f"Added {track.title} - {track.ratingKey}. {i} of {lib_size}")
        i += 1
    logger.info(f"Made a list of all track data: {lib_size} in all")
    return track_list


def export_track_data(track_data, filename):
    """
    Exports the track data to a CSV file.

    Parameters:
    track_data (list): A list of dictionaries containing track data.

    Returns:
    None
    """
    with open(filename, 'a') as csvfile:
        fieldnames = ['title', 'artist', 'album', 'genre', 'added_date', 'filepath',
                      'location', 'plex_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for element in track_data:
            writer.writerow(element)
    logger.info("Exported all track data to csv!")