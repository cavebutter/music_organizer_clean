"""
Pytest configuration and fixtures for music_organizer tests.

Test Strategy:
    E2E tests using the test Plex library (Schroeder/test_music) and sandbox database.
    Tests validate the full pipeline from Plex extraction through database storage.

Supported File Types:
    Music (positive tests): flac, mp3, ogg, opus, aac, alac, m4a
    Non-music (negative tests): txt, jpg, nfo
"""
import pytest
from db import DB_PATH, DB_USER, DB_PASSWORD, TEST_DB, DB_DATABASE
from db.database import Database
from plex import (
    PLEX_USER,
    PLEX_PASSWORD,
    PLEX_SERVER_NAME,
    PLEX_TEST_SERVER_NAME,
    PLEX_TEST_LIBRARY,
    PLEX_MUSIC_LIBRARY,
)
from plexapi.myplex import MyPlexAccount


# File type constants for test assertions
SUPPORTED_AUDIO_EXTENSIONS = {'.flac', '.mp3', '.ogg', '.opus', '.aac', '.alac', '.m4a'}
NON_AUDIO_EXTENSIONS = {'.txt', '.jpg', '.nfo'}


@pytest.fixture(scope="session")
def db_test():
    """Database connection to sandbox (test database)."""
    database = Database(DB_PATH, DB_USER, DB_PASSWORD, TEST_DB)
    database.connect()
    yield database
    database.close()


@pytest.fixture(scope="session")
def db_prod():
    """Database connection to production database. Use with caution."""
    database = Database(DB_PATH, DB_USER, DB_PASSWORD, DB_DATABASE)
    database.connect()
    yield database
    database.close()


@pytest.fixture(scope="session")
def plex_account():
    """Authenticated Plex account."""
    return MyPlexAccount(PLEX_USER, PLEX_PASSWORD)


@pytest.fixture(scope="session")
def plex_test_server(plex_account):
    """Connection to test Plex server (Schroeder)."""
    return plex_account.resource(PLEX_TEST_SERVER_NAME).connect()


@pytest.fixture(scope="session")
def plex_prod_server(plex_account):
    """Connection to production Plex server (UNRAID). Use with caution."""
    try:
        return plex_account.resource(PLEX_SERVER_NAME).connect()
    except Exception as e:
        pytest.skip(f"Production server unavailable: {e}")


@pytest.fixture(scope="session")
def test_library(plex_test_server):
    """Test music library from Schroeder."""
    return plex_test_server.library.section(PLEX_TEST_LIBRARY)


@pytest.fixture(scope="session")
def prod_library(plex_prod_server):
    """Production music library from UNRAID. Use with caution."""
    if plex_prod_server is None:
        pytest.skip("Production server unavailable")
    return plex_prod_server.library.section(PLEX_MUSIC_LIBRARY)
