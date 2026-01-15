import subprocess as s
import os
from typing import Optional
from loguru import logger
import json
from dotenv import load_dotenv

load_dotenv()

# Path mapping configuration from environment
MUSIC_PATH_PREFIX_PLEX = os.getenv("MUSIC_PATH_PREFIX_PLEX", "")
MUSIC_PATH_PREFIX_LOCAL = os.getenv("MUSIC_PATH_PREFIX_LOCAL", "")
MUSIC_PATH_PREFIX_PLEX_TEST = os.getenv("MUSIC_PATH_PREFIX_PLEX_TEST", "")
MUSIC_PATH_PREFIX_LOCAL_TEST = os.getenv("MUSIC_PATH_PREFIX_LOCAL_TEST", "")


def check_ffprobe_available() -> bool:
    """
    Check if ffprobe is installed and accessible.

    Returns:
        True if ffprobe is available, False otherwise
    """
    try:
        result = s.run(
            ["ffprobe", "-version"],
            stdout=s.PIPE,
            stderr=s.PIPE,
            text=True
        )
        if result.returncode == 0:
            logger.debug("ffprobe is available")
            return True
        else:
            logger.warning("ffprobe returned non-zero exit code")
            return False
    except FileNotFoundError:
        logger.error("ffprobe not found - please install ffmpeg")
        return False
    except Exception as e:
        logger.error(f"Error checking ffprobe availability: {e}")
        return False


def validate_path_mapping(use_test: bool = False) -> dict:
    """
    Validate that path mapping is configured and paths are accessible.

    Args:
        use_test: If True, validate test path mapping; otherwise validate production

    Returns:
        dict with keys:
            'configured': bool - env vars are set
            'accessible': bool - local path exists and is readable
            'plex_prefix': str - the Plex path prefix
            'local_prefix': str - the local path prefix
            'sample_file_ok': bool - at least one file is accessible (if accessible=True)
            'errors': list[str] - any error messages
    """
    result = {
        'configured': False,
        'accessible': False,
        'plex_prefix': '',
        'local_prefix': '',
        'sample_file_ok': False,
        'errors': []
    }

    if use_test:
        plex_prefix = MUSIC_PATH_PREFIX_PLEX_TEST
        local_prefix = MUSIC_PATH_PREFIX_LOCAL_TEST
        env_name = "test"
    else:
        plex_prefix = MUSIC_PATH_PREFIX_PLEX
        local_prefix = MUSIC_PATH_PREFIX_LOCAL
        env_name = "production"

    result['plex_prefix'] = plex_prefix
    result['local_prefix'] = local_prefix

    # Check if env vars are configured
    if not plex_prefix:
        result['errors'].append(f"MUSIC_PATH_PREFIX_PLEX{'_TEST' if use_test else ''} not configured")
    if not local_prefix:
        result['errors'].append(f"MUSIC_PATH_PREFIX_LOCAL{'_TEST' if use_test else ''} not configured")

    if not plex_prefix or not local_prefix:
        logger.warning(f"Path mapping not configured for {env_name}")
        return result

    result['configured'] = True

    # Check if local path exists
    if os.path.isdir(local_prefix):
        result['accessible'] = True
        logger.info(f"Local path accessible: {local_prefix}")

        # Try to find at least one audio file to verify read access
        for root, dirs, files in os.walk(local_prefix):
            for f in files:
                if f.lower().endswith(('.mp3', '.flac', '.m4a')):
                    test_file = os.path.join(root, f)
                    if os.access(test_file, os.R_OK):
                        result['sample_file_ok'] = True
                        logger.debug(f"Sample file accessible: {test_file}")
                        break
            if result['sample_file_ok']:
                break

        if not result['sample_file_ok']:
            result['errors'].append(f"No readable audio files found in {local_prefix}")
            logger.warning(f"No readable audio files found in {local_prefix}")
    else:
        result['errors'].append(f"Local path not accessible: {local_prefix}")
        logger.warning(f"Local path not accessible: {local_prefix} - is the mount available?")

    return result


def map_plex_path_to_local(plex_path: str, use_test: bool = False) -> Optional[str]:
    """
    Map a Plex-stored filepath to a locally accessible path.

    Args:
        plex_path: Path as stored in database (from Plex)
        use_test: If True, use test path mapping; otherwise use production

    Returns:
        Local path if mapping succeeds, None if path cannot be mapped

    Example:
        Plex path: /volume1/media/music/test-library/Artist/Album/track.flac
        Local path: /mnt/synology-temp/music/test-library/Artist/Album/track.flac
    """
    if use_test:
        prefix_from = MUSIC_PATH_PREFIX_PLEX_TEST
        prefix_to = MUSIC_PATH_PREFIX_LOCAL_TEST
    else:
        prefix_from = MUSIC_PATH_PREFIX_PLEX
        prefix_to = MUSIC_PATH_PREFIX_LOCAL

    if not prefix_from or not prefix_to:
        logger.debug(f"Path mapping not configured (use_test={use_test})")
        return None

    if not plex_path:
        return None

    if plex_path.startswith(prefix_from):
        local_path = plex_path.replace(prefix_from, prefix_to, 1)
        return local_path
    else:
        logger.debug(f"Path does not match expected prefix '{prefix_from}': {plex_path[:80]}...")
        return None


def verify_path_accessible(filepath: str) -> bool:
    """
    Check if a file path exists and is readable.

    Args:
        filepath: Full path to the file

    Returns:
        True if file exists and is readable, False otherwise
    """
    if not filepath:
        return False
    return os.path.isfile(filepath) and os.access(filepath, os.R_OK)

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