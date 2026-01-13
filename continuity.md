# Continuity Document - Music Organizer Project

**Date:** 2026-01-12
**Status:** Migrating to fresh PyCharm project

---

## What We Completed This Session

### 1. Centralized Logging System
Created `config/logging.py` with:
- `setup_logging(log_file, level, rotation, retention)` - configures loguru
- Format includes `module:function:line` for tracing issues in complex pipelines
- File rotation (10 MB default) and retention (7 days)
- Compressed rotated logs

**Files created:**
- `config/__init__.py`
- `config/logging.py`

**Files updated to use centralized logging:**
- `test/e2e_test.py`
- `test/e2e2.py`
- `test/lib_test.py`
- `test/db_update_test.py`
- `bpm_test.py`

### 2. AcousticBrainz Integration (COMPLETE)
Created `analysis/acousticbrainz.py` with:
- `get_bpm_by_mbid(mbid: str) -> Optional[float]` - single lookup
- `bulk_get_bpm(mbids: List[str]) -> Dict[str, float]` - batch lookup (25 per request)
- `fetch_bpm_for_tracks(tracks, use_bulk=True)` - process list of (track_id, mbid) tuples

Added to `db/db_update.py`:
- `process_bpm_acousticbrainz(database)` - full workflow: query DB → fetch BPMs → update DB → return stats

**API tested and working** - verified against live AcousticBrainz API.

### 3. Test Script Created
`test/test_acousticbrainz.py` - tests the full AcousticBrainz pipeline:
1. Reports MBID/BPM coverage stats
2. Shows sample tracks with MBIDs
3. Runs AcousticBrainz lookup
4. Reports hit rate and updates

### 4. Requirements.txt Updated
- Commented out librosa and dependencies (numba, llvmlite, etc.) - incompatible with Python 3.13
- Organized into "Core dependencies" and "Local BPM analysis" sections
- librosa deps will be replaced with essentia in Phase 2

### 5. Confirmed: No File Conversion Needed
- `convert_m4a_to_wav` in ffmpeg.py was only for librosa
- AcousticBrainz uses API lookups (no file I/O)
- Essentia can read M4A directly (no conversion needed)

---

## Current Blockers

### 1. Missing config.ini
The `db/__init__.py` reads from `config.ini` which is in `.gitignore` and was lost.

**Template to recreate:**
```ini
[MYSQL]
db_path = localhost
db_user = your_user
db_pwd = your_password
db_database = music_organizer
db_port = 3306

[MYSQL_TEST]
db_database = sandbox

[PLEX]
server_url = http://localhost:32400
token = your_plex_token
music_library = Music
test_library = Test Music

[LASTFM]
api_key = your_key
shared_secret = your_secret
username = your_username
password = your_password
app_name = music_organizer

[DISCOGS]
token = your_token
```

**NOT needed:**
- Spotify (API restricted Nov 2024, not available for new apps)
- MusicBrainz API key (we get MBIDs from file metadata via ffprobe)

### 2. PyCharm/Repo Issues
User is creating fresh PyCharm project and copying files over.

### 3. Test Plex Server
Need small test library (~50-100 tracks) instead of 35k production files.

---

## BPM Strategy (Simplified)

```
AcousticBrainz (60-80%) → Essentia local processing (20-40%)
```

**No Spotify** - API access restricted, not worth pursuing.

### Phase 1: AcousticBrainz (CODE COMPLETE)
- Module: `analysis/acousticbrainz.py` ✓
- DB function: `db/db_update.py:process_bpm_acousticbrainz()` ✓
- Test script: `test/test_acousticbrainz.py` ✓
- **Needs:** config.ini + test database to run

### Phase 2: Essentia Worker (NOT STARTED)
- Replace librosa with essentia
- Processes remaining tracks without AcousticBrainz data
- Can read M4A directly (no conversion needed)

---

## File Structure

```
music_organizer/
├── analysis/
│   ├── acousticbrainz.py    # NEW - AcousticBrainz BPM lookup
│   ├── bpm.py               # OLD - librosa (to be replaced)
│   ├── discogs.py
│   ├── ffmpeg.py
│   └── lastfm.py
├── config/
│   ├── __init__.py          # NEW - exports setup_logging
│   └── logging.py           # NEW - centralized logging
├── db/
│   ├── __init__.py          # Reads config.ini
│   ├── database.py
│   ├── db_functions.py
│   ├── db_update.py         # UPDATED - added process_bpm_acousticbrainz()
│   └── useful_queries.py
├── maint/
├── plex/
├── test/
│   ├── test_acousticbrainz.py  # NEW - test script
│   ├── e2e_test.py             # UPDATED - uses setup_logging
│   ├── e2e2.py                 # UPDATED - uses setup_logging
│   └── ...
├── config.ini               # MISSING - needs recreation
├── requirements.txt         # UPDATED - librosa commented out
└── continuity.md
```

---

## To Resume

1. **Create fresh PyCharm project**
2. **Copy files from this repo**
3. **Create config.ini** with MySQL and Plex credentials
4. **Create Python 3.13 venv:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
5. **Set up test Plex server** with small music library
6. **Run test:** `python test/test_acousticbrainz.py`
7. **If MBID coverage is low**, run e2e2.py first to extract MBIDs via ffprobe

---

## Database Schema (Unchanged)

```sql
track_data (id, title, artist, album, bpm, musicbrainz_id, artist_id, plex_id, filepath, location, genre)
artists (id, artist, musicbrainz_id, last_fm_id, discogs_id)
genres (id, genre)
track_genres (track_id, genre_id)
artist_genres (artist_id, genre_id)
similar_artists (artist_id, similar_artist_id)
```

Test database schema: `sandbox`

---

## Virtual Environment

**Python version:** 3.13 (librosa incompatible, commented out in requirements.txt)
**Key packages:** loguru, requests, mysql-connector-python, pandas, PlexAPI

---

## Key Findings This Session

1. **AcousticBrainz API still works** - tested successfully
2. **Spotify is dead** for new apps (Audio Features API restricted Nov 2024)
3. **No file conversion needed** for BPM workflow (AcousticBrainz = API, Essentia = reads M4A)
4. **librosa requires Python <3.13** - will skip and use essentia instead
