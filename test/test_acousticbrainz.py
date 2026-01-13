"""
Test script for AcousticBrainz BPM lookup against test database.

This script:
1. Checks current MBID coverage in the database
2. Runs AcousticBrainz BPM lookup for tracks with MBIDs but no BPM
3. Reports results and statistics
"""
import db.db_update as dbu
from config import setup_logging
from loguru import logger


def get_mbid_coverage(database):
    """Get statistics on MBID coverage in the database."""
    database.connect()

    total_tracks = database.execute_select_query(
        "SELECT COUNT(*) FROM track_data"
    )[0][0]

    tracks_with_mbid = database.execute_select_query(
        "SELECT COUNT(*) FROM track_data WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''"
    )[0][0]

    tracks_with_bpm = database.execute_select_query(
        "SELECT COUNT(*) FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
    )[0][0]

    tracks_needing_bpm = database.execute_select_query("""
        SELECT COUNT(*) FROM track_data
        WHERE musicbrainz_id IS NOT NULL
          AND musicbrainz_id != ''
          AND (bpm IS NULL OR bpm = 0)
    """)[0][0]

    database.close()

    return {
        'total_tracks': total_tracks,
        'tracks_with_mbid': tracks_with_mbid,
        'tracks_with_bpm': tracks_with_bpm,
        'tracks_needing_bpm': tracks_needing_bpm,
        'mbid_coverage_pct': (tracks_with_mbid / total_tracks * 100) if total_tracks > 0 else 0,
        'bpm_coverage_pct': (tracks_with_bpm / total_tracks * 100) if total_tracks > 0 else 0
    }


def show_sample_mbids(database, limit=5):
    """Show sample MBIDs from the database for verification."""
    database.connect()

    samples = database.execute_select_query(f"""
        SELECT id, title, artist, musicbrainz_id
        FROM track_data
        WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''
        LIMIT {limit}
    """)

    database.close()
    return samples


if __name__ == '__main__':
    setup_logging("logs/test_acousticbrainz.log")

    logger.info("=" * 60)
    logger.info("AcousticBrainz BPM Lookup Test")
    logger.info("=" * 60)

    db = dbu.database

    # Step 1: Check current coverage
    logger.info("\n--- Step 1: Checking current database coverage ---")
    coverage = get_mbid_coverage(db)

    print(f"\nDatabase Statistics:")
    print(f"  Total tracks:        {coverage['total_tracks']}")
    print(f"  Tracks with MBID:    {coverage['tracks_with_mbid']} ({coverage['mbid_coverage_pct']:.1f}%)")
    print(f"  Tracks with BPM:     {coverage['tracks_with_bpm']} ({coverage['bpm_coverage_pct']:.1f}%)")
    print(f"  Tracks needing BPM:  {coverage['tracks_needing_bpm']}")

    if coverage['tracks_needing_bpm'] == 0:
        if coverage['tracks_with_mbid'] == 0:
            print("\nNo tracks have MusicBrainz IDs yet.")
            print("Run the ffmpeg MBID extraction first (see e2e2.py)")
        else:
            print("\nAll tracks with MBIDs already have BPM values!")
        exit(0)

    # Step 2: Show sample MBIDs
    logger.info("\n--- Step 2: Sample MBIDs from database ---")
    samples = show_sample_mbids(db)

    if samples:
        print(f"\nSample tracks with MBIDs:")
        for track_id, title, artist, mbid in samples:
            print(f"  [{track_id}] {artist} - {title}")
            print(f"        MBID: {mbid}")
    else:
        print("\nNo tracks with MBIDs found in database.")
        exit(0)

    # Step 3: Run AcousticBrainz lookup
    logger.info("\n--- Step 3: Running AcousticBrainz BPM lookup ---")
    print(f"\nProcessing {coverage['tracks_needing_bpm']} tracks...")
    print("(This may take a while depending on the number of tracks)\n")

    stats = dbu.process_bpm_acousticbrainz(db)

    # Step 4: Report results
    logger.info("\n--- Step 4: Results ---")
    print(f"\nAcousticBrainz Lookup Results:")
    print(f"  Tracks processed:  {stats['total']}")
    print(f"  BPM found (hits):  {stats['hits']}")
    print(f"  BPM not found:     {stats['misses']}")
    print(f"  Database updated:  {stats['updated']}")
    if stats['total'] > 0:
        print(f"  Hit rate:          {stats['hits']/stats['total']*100:.1f}%")

    # Step 5: Show final coverage
    logger.info("\n--- Step 5: Final coverage ---")
    final_coverage = get_mbid_coverage(db)

    print(f"\nFinal Database Statistics:")
    print(f"  Tracks with BPM:     {final_coverage['tracks_with_bpm']} ({final_coverage['bpm_coverage_pct']:.1f}%)")
    print(f"  Tracks still needing BPM: {final_coverage['tracks_needing_bpm']}")

    improvement = final_coverage['tracks_with_bpm'] - coverage['tracks_with_bpm']
    print(f"\n  BPM coverage improved by {improvement} tracks!")

    logger.info("Test complete!")