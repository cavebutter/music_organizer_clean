# Fuck unit testing
# Double fuck AI generated unit testing

import pytest
from unittest.mock import patch, MagicMock
from db.database import Database

@pytest.fixture
@patch('db.database.mysql.connector.connect')
def db(mock_connect):
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    db_instance = Database('localhost', 'user', 'password', 'test_db')
    db_instance.connect()
    return db_instance, mock_connection

def test_connect(db):
    db_instance, mock_connection = db
    assert db_instance.connection is not None
    mock_connection.cursor.assert_not_called()

def test_close(db):
    db_instance, mock_connection = db
    db_instance.close()
    mock_connection.close.assert_called_once()
    assert db_instance.connection is None

@patch('db.database.mysql.connector.connect')
def test_drop_table(mock_connect, db):
    db_instance, mock_connection = db
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    db_instance.drop_table('test_table')

    mock_cursor.execute.assert_called_once_with('DROP TABLE IF EXISTS test_table')
    mock_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()

@patch('db.database.mysql.connector.connect')
def test_create_table(mock_connect, db):
    db_instance, mock_connection = db
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    db_instance.create_table('CREATE TABLE test (id INT)')

    mock_cursor.execute.assert_called_once_with('CREATE TABLE test (id INT)')
    mock_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()

@patch('db.database.mysql.connector.connect')
def test_execute_query(mock_connect, db):
    db_instance, mock_connection = db
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    db_instance.execute_query('INSERT INTO test (id) VALUES (1)')

    mock_cursor.execute.assert_called_once_with('INSERT INTO test (id) VALUES (1)')
    mock_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()

@patch('db.database.mysql.connector.connect')
def test_execute_select_query(mock_connect, db):
    db_instance, mock_connection = db
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [(1,)]

    result = db_instance.execute_select_query('SELECT * FROM test')

    mock_cursor.execute.assert_called_once_with('SELECT * FROM test')
    assert result == [(1,)]
    mock_cursor.close.assert_called_once()

@patch('db.database.mysql.connector.connect')
def test_create_artists_table(mock_connect, db):
    db_instance, mock_connection = db

    db_instance.create_artists_table()

    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 0')
    mock_connection.cursor().execute.assert_any_call('DROP TABLE IF EXISTS artists')
    mock_connection.cursor().execute.assert_any_call('CREATE TABLE IF NOT EXISTS artists(\n        id INTEGER PRIMARY KEY AUTO_INCREMENT\n        , artist VARCHAR(255) NOT NULL\n        , last_fm_id VARCHAR(255)\n        , discogs_id VARCHAR(255)\n        , musicbrainz_id VARCHAR(255)\n        )')
    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 1')

@patch('db.database.mysql.connector.connect')
def test_create_track_data_table(mock_connect, db):
    db_instance, mock_connection = db

    db_instance.create_track_data_table()

    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 0')
    mock_connection.cursor().execute.assert_any_call('DROP TABLE IF EXISTS track_data')
    mock_connection.cursor().execute.assert_any_call('CREATE TABLE IF NOT EXISTS track_data(\n        id INTEGER PRIMARY KEY AUTO_INCREMENT\n        , title VARCHAR (1000) NOT NULL\n        , artist VARCHAR (1000) NOT NULL\n        , album VARCHAR (1000) NOT NULL\n        , added_date VARCHAR (50)\n        , filepath VARCHAR (500)\n        , location VARCHAR (500)\n        , bpm INTEGER\n        , genre VARCHAR (1000)\n        , artist_id INTEGER\n        , Test_Server_id INTEGER\n        , schroeder_id INTEGER\n        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE)')
    mock_connection.cursor().execute.assert_any_call('CREATE INDEX ix_loc ON track_data (location)')
    mock_connection.cursor().execute.assert_any_call('CREATE INDEX ix_fileath on track_data (filepath)')
    mock_connection.cursor().execute.assert_any_call('CREATE INDEX ix_bpm on track_data (bpm)')
    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 1')

@patch('db.database.mysql.connector.connect')
def test_create_history_table(mock_connect, db):
    db_instance, mock_connection = db

    db_instance.create_history_table()

    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 0')
    mock_connection.cursor().execute.assert_any_call('DROP TABLE IF EXISTS history')
    mock_connection.cursor().execute.assert_any_call('CREATE TABLE IF NOT EXISTS history(\n        id INTEGER PRIMARY KEY AUTO_INCREMENT\n        , tx_date VARCHAR (255)\n        , records INTEGER (6))')
    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 1')

@patch('db.database.mysql.connector.connect')
def test_create_tags_table(mock_connect, db):
    db_instance, mock_connection = db

    db_instance.create_tags_table()

    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 0')
    mock_connection.cursor().execute.assert_any_call('DROP TABLE IF EXISTS tags')
    mock_connection.cursor().execute.assert_any_call('CREATE TABLE IF NOT EXISTS tags(\n        id INTEGER PRIMARY KEY AUTO_INCREMENT\n        , tag INTEGER (6)\n        , artist_id INTEGER\n\n        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE\n        , FOREIGN KEY (tag) REFERENCES genres(id) ON DELETE CASCADE)')
    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 1')

@patch('db.database.mysql.connector.connect')
def test_create_similar_artists_table(mock_connect, db):
    db_instance, mock_connection = db

    db_instance.create_similar_artists_table()

    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 0')
    mock_connection.cursor().execute.assert_any_call('DROP TABLE IF EXISTS similar_artists')
    mock_connection.cursor().execute.assert_any_call('CREATE TABLE IF NOT EXISTS similar_artists(\n        id INTEGER PRIMARY KEY AUTO_INCREMENT\n        , artist_id INTEGER\n        , similar_artist_id INTEGER\n        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE\n        , FOREIGN KEY (similar_artist_id) REFERENCES artists(id) ON DELETE CASCADE)')
    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 1')

@patch('db.database.mysql.connector.connect')
def test_create_genres_table(mock_connect, db):
    db_instance, mock_connection = db

    db_instance.create_genres_table()

    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 0')
    mock_connection.cursor().execute.assert_any_call('DROP TABLE IF EXISTS genres')
    mock_connection.cursor().execute.assert_any_call('CREATE TABLE IF NOT EXISTS genres(\n        id INTEGER PRIMARY KEY AUTO_INCREMENT\n        , genre VARCHAR(1000) NOT NULL\n        )')
    mock_connection.cursor().execute.assert_any_call('SET FOREIGN_KEY_CHECKS = 1')