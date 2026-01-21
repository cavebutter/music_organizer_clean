"""
Integration tests for the BPM enrichment pipeline.

Tests the full flow: sandbox DB -> AcousticBrainz API -> sandbox DB update.
"""

import pytest

import db.db_update as dbu


class TestProcessBpmAcousticbrainz:
    """Tests for the full BPM pipeline."""

    def test_returns_stats_dict(self, db_test):
        """Should return a dict with expected stats keys."""
        stats = dbu.process_bpm_acousticbrainz(db_test)

        assert isinstance(stats, dict)
        assert "total" in stats
        assert "hits" in stats
        assert "misses" in stats
        assert "updated" in stats

    def test_stats_values_are_integers(self, db_test):
        """All stats should be non-negative integers."""
        stats = dbu.process_bpm_acousticbrainz(db_test)

        for _key, value in stats.items():
            assert isinstance(value, int)
            assert value >= 0

    def test_hits_plus_misses_equals_total(self, db_test):
        """Hits + misses should equal total processed."""
        stats = dbu.process_bpm_acousticbrainz(db_test)

        assert stats["hits"] + stats["misses"] == stats["total"]

    def test_updated_not_greater_than_hits(self, db_test):
        """Can't update more tracks than we got hits for."""
        stats = dbu.process_bpm_acousticbrainz(db_test)

        assert stats["updated"] <= stats["hits"]


class TestBpmPipelineWithFreshData:
    """Tests that clear and repopulate BPM data. Requires track_data to have entries."""

    @pytest.fixture
    def clear_bpm(self, db_test):
        """Clear all BPM values before test, restore after."""
        db_test.connect()

        # Check if there are tracks with MBIDs
        tracks_with_mbid = db_test.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''"
        )[0][0]

        if tracks_with_mbid == 0:
            db_test.close()
            pytest.skip("No tracks with MBIDs in sandbox - run e2e pipeline first")

        # Get current state
        original = db_test.execute_select_query(
            "SELECT id, bpm FROM track_data WHERE bpm IS NOT NULL"
        )

        # Clear BPM
        db_test.execute_query("UPDATE track_data SET bpm = NULL")
        db_test.close()

        yield tracks_with_mbid

        # Restore original values
        db_test.connect()
        for track_id, bpm in original:
            if bpm:
                db_test.execute_query(
                    "UPDATE track_data SET bpm = %s WHERE id = %s", (bpm, track_id)
                )
        db_test.close()

    def test_populates_bpm_from_empty(self, db_test, clear_bpm):
        """Should populate BPM values when starting from empty."""
        # Verify BPM is cleared
        db_test.connect()
        before = db_test.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
        )[0][0]
        db_test.close()
        assert before == 0

        # Run the pipeline
        stats = dbu.process_bpm_acousticbrainz(db_test)

        # Verify BPM was populated
        db_test.connect()
        after = db_test.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
        )[0][0]
        db_test.close()

        assert after > 0
        assert after == stats["updated"]

    def test_high_hit_rate_for_known_tracks(self, db_test, clear_bpm):
        """Sandbox contains well-known artists, should have high hit rate."""
        stats = dbu.process_bpm_acousticbrainz(db_test)

        if stats["total"] > 0:
            hit_rate = stats["hits"] / stats["total"]
            # Expect at least 80% hit rate for well-known music
            assert hit_rate >= 0.8, f"Hit rate {hit_rate:.1%} below 80%"

    def test_bpm_values_in_valid_range(self, db_test, clear_bpm):
        """All BPM values should be in reasonable range."""
        dbu.process_bpm_acousticbrainz(db_test)

        db_test.connect()
        bpm_values = db_test.execute_select_query(
            "SELECT bpm FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
        )
        db_test.close()

        for (bpm,) in bpm_values:
            assert 40 <= bpm <= 220, f"BPM {bpm} outside valid range"
