"""
Reset the sandbox (test) database by truncating all tables.

Preserves table schema but removes all data for a fresh test run.
"""

from loguru import logger

from db import DB_PASSWORD, DB_PATH, DB_USER, TEST_DB
from db.database import Database

# Tables to truncate, in order (respects foreign key constraints)
TABLES_TO_TRUNCATE = [
    "track_genres",
    "artist_genres",
    "similar_artists",
    "track_data",
    "artists",
    "genres",
    "history",
]


def truncate_all_tables(database: Database) -> int:
    """
    Truncate all tables in the test database.

    Args:
        database: Connected Database instance

    Returns:
        Number of tables truncated
    """
    database.connect()

    # Disable foreign key checks for truncation
    database.execute_query("SET FOREIGN_KEY_CHECKS = 0")

    truncated = 0
    for table in TABLES_TO_TRUNCATE:
        try:
            database.execute_query(f"TRUNCATE TABLE {table}")
            logger.info(f"Truncated table: {table}")
            truncated += 1
        except Exception as e:
            logger.warning(f"Could not truncate {table}: {e}")

    # Re-enable foreign key checks
    database.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    database.close()
    return truncated


if __name__ == "__main__":
    print(f"Resetting sandbox database: {TEST_DB}")

    db = Database(DB_PATH, DB_USER, DB_PASSWORD, TEST_DB)
    count = truncate_all_tables(db)

    print(f"Truncated {count} tables")
