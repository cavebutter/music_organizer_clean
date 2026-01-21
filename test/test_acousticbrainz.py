"""
Integration tests for AcousticBrainz BPM lookup.

These tests hit the real AcousticBrainz API using MBIDs from the sandbox database.
"""

from analysis.acousticbrainz import bulk_get_bpm, fetch_bpm_for_tracks, get_bpm_by_mbid

# Known MBIDs from sandbox (The Smiths, Rush, XTC)
KNOWN_MBIDS = {
    "28aac9af-f7e2-40cf-a7b6-c40e03d6cf3f": "Rush - Circumstances",
    "3d120ebc-51e0-4cfb-89fd-8f432ed0565c": "Rush - The Trees",
    "2dfa974a-9664-494f-b01b-da3a9eb64765": "Rush - La Villa Strangiato",
}


class TestGetBpmByMbid:
    """Tests for single MBID lookup."""

    def test_valid_mbid_returns_bpm(self):
        """Known MBID should return a BPM value."""
        mbid = "28aac9af-f7e2-40cf-a7b6-c40e03d6cf3f"  # Rush - Circumstances
        bpm = get_bpm_by_mbid(mbid)

        assert bpm is not None
        assert isinstance(bpm, float)
        assert 60 <= bpm <= 200  # Reasonable BPM range

    def test_invalid_mbid_returns_none(self):
        """Invalid MBID should return None, not raise."""
        bpm = get_bpm_by_mbid("00000000-0000-0000-0000-000000000000")
        assert bpm is None

    def test_malformed_mbid_returns_none(self):
        """Malformed MBID should return None, not raise."""
        bpm = get_bpm_by_mbid("not-a-valid-mbid")
        assert bpm is None

    def test_empty_mbid_returns_none(self):
        """Empty MBID should return None, not raise."""
        bpm = get_bpm_by_mbid("")
        assert bpm is None


class TestBulkGetBpm:
    """Tests for bulk MBID lookup."""

    def test_bulk_lookup_returns_dict(self):
        """Bulk lookup should return a dict of MBID -> BPM."""
        mbids = list(KNOWN_MBIDS.keys())
        results = bulk_get_bpm(mbids)

        assert isinstance(results, dict)
        assert len(results) > 0  # At least some hits

    def test_bulk_lookup_values_are_floats(self):
        """All BPM values should be floats."""
        mbids = list(KNOWN_MBIDS.keys())
        results = bulk_get_bpm(mbids)

        for _mbid, bpm in results.items():
            assert isinstance(bpm, float)
            assert 60 <= bpm <= 200

    def test_bulk_lookup_empty_list(self):
        """Empty list should return empty dict."""
        results = bulk_get_bpm([])
        assert results == {}

    def test_bulk_lookup_all_invalid(self):
        """All invalid MBIDs should return empty dict."""
        invalid_mbids = [
            "00000000-0000-0000-0000-000000000000",
            "11111111-1111-1111-1111-111111111111",
        ]
        results = bulk_get_bpm(invalid_mbids)
        assert results == {}


class TestFetchBpmForTracks:
    """Tests for the track-based lookup wrapper."""

    def test_fetch_returns_dict_with_track_ids(self):
        """Results should map track_id -> BPM."""
        # Simulate track data: (track_id, mbid)
        tracks = [
            (1, "28aac9af-f7e2-40cf-a7b6-c40e03d6cf3f"),
            (2, "3d120ebc-51e0-4cfb-89fd-8f432ed0565c"),
        ]
        results = fetch_bpm_for_tracks(tracks, use_bulk=True)

        assert isinstance(results, dict)
        # Track IDs should be keys, not MBIDs
        for key in results:
            assert isinstance(key, int)

    def test_fetch_empty_list(self):
        """Empty track list should return empty dict."""
        results = fetch_bpm_for_tracks([], use_bulk=True)
        assert results == {}

    def test_fetch_single_mode(self):
        """Single mode should also work."""
        tracks = [(1, "28aac9af-f7e2-40cf-a7b6-c40e03d6cf3f")]
        results = fetch_bpm_for_tracks(tracks, use_bulk=False)

        assert isinstance(results, dict)
        if results:  # May or may not hit depending on API
            assert 1 in results
