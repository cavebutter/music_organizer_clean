import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MYSQL_HOST", "localhost")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
DB_DATABASE = os.getenv("MYSQL_DATABASE", "music_organizer")
TEST_DB = os.getenv("MYSQL_TEST_DATABASE", "sandbox")
DB_PORT = os.getenv("MYSQL_PORT", "3306")

__all__ = [
    'DB_PATH',
    'DB_USER',
    'DB_PASSWORD',
    'DB_DATABASE',
    'TEST_DB',
    'DB_PORT'
]