import librosa
from loguru import logger
import os
import soundfile as sf
import warnings
import audioread
import numpy as np
import traceback

# Suppress audioread deprecation warnings in stdout
warnings.filterwarnings("ignore", category=DeprecationWarning, module='audioread')


def get_bpm(audio_file):
    """
    Calculate the beats per minute (BPM) of the provided audio file.

    Parameters:
    audio_file (str): The path to the audio file.

    Returns:
    int or None: The BPM value calculated from the audio file, or None if processing fails.
    """
    # Check file existence
    if not os.path.exists(audio_file):
        logger.error(f"File not found: {audio_file}")
        return None

    try:
        # Check if file is m4a and handle it with audioread
        if audio_file.lower().endswith('.m4a'):
            with audioread.audio_open(audio_file) as f:
                y, sr = librosa.load(f)
        else:
            # Default loading for other formats
            y, sr = librosa.load(audio_file, duration=180)

        # Validate audio data
        if y.size == 0 or sr == 0:
            logger.error(f"Invalid audio data in file: {audio_file}")
            return None

        # Calculate BPM
        bpm = librosa.beat.beat_track(y=y, sr=sr)[0]
        bpm = int(bpm)
        logger.info(f"Calculated BPM: {bpm} for {audio_file}")
        return bpm

    except sf.LibsndfileError as e:
        logger.error(f"PySoundFile error processing {audio_file}: {str(e)}")
        return None
    except (UserWarning, FutureWarning) as w:
        logger.warning(f"Warning processing {audio_file}: {str(w)}")
        return None
    except Exception as e:
        logger.error(f"Error processing {audio_file}: {str(e)}")
        logger.debug(traceback.format_exc())
        return None


def bpm_cleanup():
    """
    For use after initial setup or after update from Plex.
    Query db to get id, plex_id, and location for all tracks that are filetype '.m4a' and have no BPM.
    Make a copy of the file, convert to .wav, get BPM, update db, delete .wav file.
    Returns:

    """
    pass
