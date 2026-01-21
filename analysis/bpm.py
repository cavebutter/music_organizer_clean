"""
BPM (tempo) analysis for audio files using Essentia.

This module provides local BPM detection for tracks that don't have BPM data
from AcousticBrainz. It's designed to run as Phase 7.2 in the pipeline,
after AcousticBrainz lookup (Phase 7.1).
"""

import os

from loguru import logger

try:
    import essentia.standard as es

    ESSENTIA_AVAILABLE = True
except ImportError:
    ESSENTIA_AVAILABLE = False
    logger.warning("Essentia not installed - local BPM analysis unavailable")


def check_essentia_available() -> bool:
    """
    Check if Essentia is installed and available.

    Returns:
        True if Essentia is available, False otherwise
    """
    return ESSENTIA_AVAILABLE


def get_bpm_essentia(filepath: str) -> float | None:
    """
    Calculate BPM for an audio file using Essentia's RhythmExtractor2013.

    Args:
        filepath: Path to the audio file (supports mp3, flac, m4a, wav, etc.)

    Returns:
        BPM as float if successful, None on error

    Note:
        RhythmExtractor2013 is Essentia's recommended algorithm for BPM detection.
        It returns BPM along with confidence score, beat positions, and estimates.
    """
    if not ESSENTIA_AVAILABLE:
        logger.error("Essentia not available - cannot analyze BPM")
        return None

    if not filepath:
        logger.debug("Empty filepath provided")
        return None

    if not os.path.isfile(filepath):
        logger.debug(f"File not found: {filepath}")
        return None

    try:
        # MonoLoader handles various formats and resamples to 44100Hz mono
        loader = es.MonoLoader(filename=filepath)
        audio = loader()

        if len(audio) == 0:
            logger.warning(f"Empty audio data from file: {filepath}")
            return None

        # RhythmExtractor2013 is the recommended BPM detection algorithm
        rhythm_extractor = es.RhythmExtractor2013()
        bpm, ticks, confidence, estimates, intervals = rhythm_extractor(audio)

        # Validate BPM is in reasonable range (40-220 BPM covers most music)
        if bpm < 40 or bpm > 220:
            logger.warning(f"BPM {bpm:.2f} outside valid range for {filepath}")
            # Still return it - let the caller decide
            return float(bpm)

        logger.debug(
            f"BPM: {bpm:.2f} (confidence: {confidence:.2f}) for {os.path.basename(filepath)}"
        )
        return float(bpm)

    except RuntimeError as e:
        # Essentia raises RuntimeError for file format issues
        logger.debug(f"Essentia error processing {filepath}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error analyzing {filepath}: {e}")
        return None


def get_bpm_with_confidence(filepath: str) -> tuple[float | None, float | None]:
    """
    Calculate BPM and confidence score for an audio file.

    Args:
        filepath: Path to the audio file

    Returns:
        Tuple of (bpm, confidence), both may be None on error
    """
    if not ESSENTIA_AVAILABLE:
        return None, None

    if not filepath or not os.path.isfile(filepath):
        return None, None

    try:
        loader = es.MonoLoader(filename=filepath)
        audio = loader()

        if len(audio) == 0:
            return None, None

        rhythm_extractor = es.RhythmExtractor2013()
        bpm, ticks, confidence, estimates, intervals = rhythm_extractor(audio)

        return float(bpm), float(confidence)

    except Exception as e:
        logger.debug(f"Error analyzing {filepath}: {e}")
        return None, None
