import db.db_update as dbu
import subprocess as s
import os
from loguru import logger
import json

db = dbu.database

def ffmpeg_get_info(track) -> json:
    """

    Args:
        track: Audio file path

    Returns: json with ffmpeg info

    """
    logger.debug(f"Getting ffmpeg info for {track}")
    cmd = ["ffprobe",
               "-v", "error",
               "-print_format", "json",
                "-show_format",
                "-show_streams",
                track]
    result = s.run(cmd, stdout=s.PIPE, stderr=s.PIPE, text=True)

    if result.returncode != 0:
        logger.error(f"Error getting ffmpeg info for {track}: {result.stderr}")
        return None
    return json.loads(result.stdout)


def ffmpeg_get_mbtid(track_info: json) -> str:
    """
    Get MusicBrainz Track ID from ffmpeg info
    Args:
        track_info: json with ffmpeg info

    Returns: mbtb

    """
    mbtid = track_info["format"]["tags"]['MusicBrainz Track Id']
    if not mbtid:
        logger.error(f"Error getting mbtid from ffmpeg info: {track_info}")
        return None
    else:
        logger.info(f"Got mbtid {mbtid} from ffmpeg info for {track_info['format']['filename']}")
        return mbtid


def ffmpeg_get_track_artist_and_artist_mbid(track_info: json) -> tuple:
    """
    Get track artist and artist id from ffmpeg info
    Args:
        track_info: json with ffmpeg info

    Returns: tuple with track artist and artist id

    """
    track_artist = track_info["format"]["tags"]['artist']
    if not track_artist:
        logger.error(f"Error getting track artist from ffmpeg info: {track_info}")
        return None, None
    else:
        logger.info(f"Got track artist {track_artist} from ffmpeg info for {track_info['format']['filename']}")
        artist_mbid = track_info["format"]["tags"]['MusicBrainz Artist Id']
        if not artist_mbid:
            logger.error(f"Error getting artist mbid from ffmpeg info: {track_info}")
            return track_artist, None
        else:
            logger.info(f"Got artist mbid {artist_mbid} from ffmpeg info for {track_info['format']['filename']}")
            return track_artist, artist_mbid


def convert_m4a_to_wav(input_file):
    """Convert an .m4a file to .wav in a persistent temp directory with logging."""
    temp_dir = "temp"
    output_file = os.path.join(temp_dir, os.path.splitext(os.path.basename(input_file))[0] + ".wav")

    logger.info(f"Starting conversion: {input_file} -> {output_file}")
    logger.debug(f"Generating ffmpeg command for conversion.")

    cmd = [
        "ffmpeg",
        "-i", input_file,
        "-acodec", "pcm_s16le",
        "-ar", "44100",
        output_file,
        "-y"
    ]

    logger.debug(f"Running command: {' '.join(cmd)}")

    try:
        result = s.run(cmd, stdout=s.PIPE, stderr=s.PIPE, text=True)

        if result.returncode != 0:
            logger.error(f"ffmpeg error processing {input_file}: {result.stderr}")
            raise Exception(f"ffmpeg error: {result.stderr}")

        logger.info(f"Successfully converted {input_file} -> {output_file}")
        return output_file

    except Exception as e:
        logger.exception(f"Exception occurred while converting {input_file}")
        return None


def cleanup_temp_file(file_path: str):
    """
    Clean up temporary files after processing.
    Args:
        file_path:

    Returns:

    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed temporary file: {file_path}")
        else:
            logger.warning(f"Temporary file not found for cleanup: {file_path}")
    except Exception as e:
        logger.error(f"Error removing temporary file {file_path}: {e}")


def insert_track_mbid(track_id: int, mbid: str):
    """
    Insert track mbid into database
    Args:
        track_id: track id
        mbid: mbid

    Returns:

    """
    db.connect()
    update_query = f"""UPDATE track_data SET mbid = '{mbid}' WHERE id = {track_id}"""
    try:
        db.execute_query(update_query)
        logger.info(f"Updated {track_id} with mbid {mbid}")
    except Exception as e:
        logger.error(f"Error updating {track_id} with mbid {mbid}: {e}")