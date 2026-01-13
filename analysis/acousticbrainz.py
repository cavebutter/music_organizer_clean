"""
AcousticBrainz API integration for BPM lookup.

AcousticBrainz is a read-only database of pre-computed audio features.
The service shut down in 2022 but the API still works for lookups.
No API key required. Data is CC0 licensed.
"""
import requests
from typing import Optional, Dict, List
from time import sleep
from loguru import logger

# AcousticBrainz API endpoints
BASE_URL = "https://acousticbrainz.org/api/v1"
SINGLE_ENDPOINT = "{base}/{mbid}/low-level"
BULK_ENDPOINT = "{base}/low-level"

# Rate limiting - be respectful to the read-only service
REQUEST_DELAY = 0.1  # 100ms between requests
BULK_BATCH_SIZE = 25  # Max MBIDs per bulk request


def get_bpm_by_mbid(mbid: str) -> Optional[float]:
    """
    Get BPM for a single track from AcousticBrainz.

    Args:
        mbid: MusicBrainz Recording ID

    Returns:
        BPM as float, or None if not found
    """
    url = SINGLE_ENDPOINT.format(base=BASE_URL, mbid=mbid)

    try:
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            bpm = data.get("rhythm", {}).get("bpm")
            if bpm:
                logger.info(f"Got BPM {bpm:.1f} for MBID {mbid}")
                return float(bpm)
            else:
                logger.warning(f"No BPM in response for MBID {mbid}")
                return None

        elif response.status_code == 404:
            logger.debug(f"No AcousticBrainz data for MBID {mbid}")
            return None
        else:
            logger.error(f"AcousticBrainz API error {response.status_code} for MBID {mbid}")
            return None

    except requests.RequestException as e:
        logger.error(f"Request failed for MBID {mbid}: {e}")
        return None


def bulk_get_bpm(mbids: List[str]) -> Dict[str, float]:
    """
    Get BPM for multiple tracks in a single request.

    Args:
        mbids: List of MusicBrainz Recording IDs (max 25)

    Returns:
        Dict mapping MBID -> BPM for successful lookups
    """
    if not mbids:
        return {}

    if len(mbids) > BULK_BATCH_SIZE:
        logger.warning(f"Bulk request exceeds {BULK_BATCH_SIZE} MBIDs, truncating")
        mbids = mbids[:BULK_BATCH_SIZE]

    # Bulk endpoint uses semicolon-separated MBIDs
    recording_ids = ";".join(mbids)
    url = BULK_ENDPOINT.format(base=BASE_URL)

    try:
        response = requests.get(
            url,
            params={"recording_ids": recording_ids},
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            results = {}

            for mbid, info in data.items():
                # Bulk response structure differs - check for the BPM data
                if isinstance(info, dict) and "0" in info:
                    # AcousticBrainz may have multiple submissions, use first
                    bpm = info["0"].get("rhythm", {}).get("bpm")
                    if bpm:
                        results[mbid] = float(bpm)

            logger.info(f"Bulk lookup: {len(results)}/{len(mbids)} hits")
            return results
        else:
            logger.error(f"Bulk API error {response.status_code}")
            return {}

    except requests.RequestException as e:
        logger.error(f"Bulk request failed: {e}")
        return {}


def fetch_bpm_for_tracks(tracks: List[tuple], use_bulk: bool = True) -> Dict[int, float]:
    """
    Fetch BPM for a list of tracks from database.

    Args:
        tracks: List of (track_id, mbid) tuples
        use_bulk: Use bulk API when possible (faster, recommended)

    Returns:
        Dict mapping track_id -> BPM for successful lookups
    """
    results = {}
    total = len(tracks)
    hits = 0
    misses = 0
    errors = 0

    logger.info(f"Starting AcousticBrainz lookup for {total} tracks")

    if use_bulk:
        # Process in batches
        for i in range(0, total, BULK_BATCH_SIZE):
            batch = tracks[i:i + BULK_BATCH_SIZE]
            mbid_to_track_id = {mbid: track_id for track_id, mbid in batch}
            mbids = list(mbid_to_track_id.keys())

            bpm_results = bulk_get_bpm(mbids)

            for mbid, bpm in bpm_results.items():
                track_id = mbid_to_track_id[mbid]
                results[track_id] = bpm
                hits += 1

            misses += len(batch) - len(bpm_results)

            # Progress logging
            processed = min(i + BULK_BATCH_SIZE, total)
            logger.info(f"Progress: {processed}/{total} ({hits} hits, {misses} misses)")

            sleep(REQUEST_DELAY)
    else:
        # Single requests (slower but more detailed logging)
        for idx, (track_id, mbid) in enumerate(tracks):
            bpm = get_bpm_by_mbid(mbid)

            if bpm:
                results[track_id] = bpm
                hits += 1
            else:
                misses += 1

            if (idx + 1) % 100 == 0:
                logger.info(f"Progress: {idx + 1}/{total} ({hits} hits, {misses} misses)")

            sleep(REQUEST_DELAY)

    logger.info(f"AcousticBrainz lookup complete: {hits} hits, {misses} misses, {errors} errors")
    logger.info(f"Hit rate: {hits/total*100:.1f}%" if total > 0 else "No tracks to process")

    return results