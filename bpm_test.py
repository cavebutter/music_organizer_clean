# This script is to be used on a different machine with local access to music library

import db.db_update as dbu
from config import setup_logging


if __name__ == '__main__':
    setup_logging("logs/bpm_test.log")
    dbu.process_bpm(dbu.database, 'output/test_id_location.csv')