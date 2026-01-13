import pytest
from unittest.mock import patch, MagicMock
from plex.plex_library import plex_connect, get_music_library, \
    get_all_tracks, get_all_tracks_limit, extract_track_data, \
    listify_track_data, export_track_data


# Arrange
PLEX_USERNAME = "test_user"
PLEX_PASSWORD = "test_password"
PLEX_SERVER = "test_server"

def test_plex_connect_success():
    # Arrange
    with patch('plex.plex_library.MyPlexAccount') as MockMyPlexAccount:
        mock_account = MockMyPlexAccount.return_value
        mock_server = MagicMock()
        mock_account.resource.return_value.connect.return_value = mock_server

        # Act
        server = plex_connect()

        # Assert
        MockMyPlexAccount.assert_called_once_with('jay@jay-cohen.info', 'bugaboo@hulking6amends')
        mock_account.resource.assert_called_once_with('Woodstock')
        mock_account.resource.return_value.connect.assert_called_once()
        assert server == mock_server

def test_plex_connect_failure():
    # Arrange
    with patch('plex.plex_library.MyPlexAccount') as MockMyPlexAccount:
        mock_account = MockMyPlexAccount.return_value
        mock_account.resource.return_value.connect.side_effect = Exception("Connection error")

        # Act & Assert
        with pytest.raises(SystemExit):
            plex_connect()

def test_plex_connect_invalid_credentials():
    # Arrange
    with patch('plex.plex_library.MyPlexAccount') as MockMyPlexAccount:
        mock_account = MockMyPlexAccount.return_value
        mock_account.resource.side_effect = Exception("Invalid credentials")

        # Act & Assert
        with pytest.raises(SystemExit):
            plex_connect()

def test_get_music_library_success():
    # Arrange
    mock_server = MagicMock()
    mock_library = MagicMock()
    mock_server.library.section.return_value = mock_library
    library_name = "Music"

    # Act
    result = get_music_library(mock_server, library_name)

    # Assert
    mock_server.library.section.assert_called_once_with(library_name)
    assert result == mock_library

def test_get_music_library_failure():
    # Arrange
    mock_server = MagicMock()
    mock_server.library.section.side_effect = Exception("Library not found")
    library_name = "NonExistentLibrary"

    # Act & Assert
    with pytest.raises(SystemExit):
        get_music_library(mock_server, library_name)

def test_get_music_library_invalid_server():
    # Arrange
    mock_server = None
    library_name = "Music"

    # Act & Assert
    with pytest.raises(SystemExit):
        get_music_library(mock_server, library_name)



def test_get_all_tracks_success():
    # Arrange
    mock_library = MagicMock()
    mock_tracks = [MagicMock(), MagicMock()]
    mock_library.searchTracks.return_value = mock_tracks

    # Act
    tracks, library_size = get_all_tracks(mock_library)

    # Assert
    mock_library.searchTracks.assert_called_once()
    assert tracks == mock_tracks
    assert library_size == len(mock_tracks)

def test_get_all_tracks_failure():
    # Arrange
    mock_library = MagicMock()
    mock_library.searchTracks.side_effect = Exception("Error retrieving tracks")

    # Act & Assert
    with pytest.raises(SystemExit):
        get_all_tracks(mock_library)

def test_get_all_tracks_empty():
    # Arrange
    mock_library = MagicMock()
    mock_library.searchTracks.return_value = []

    # Act
    tracks, library_size = get_all_tracks(mock_library)

    # Assert
    mock_library.searchTracks.assert_called_once()
    assert tracks == []
    assert library_size == 0

def test_get_all_tracks_limit_success():
    # Arrange
    mock_library = MagicMock()
    mock_tracks = [MagicMock() for _ in range(50)]
    mock_library.searchTracks.return_value = mock_tracks

    # Act
    tracks, library_size = get_all_tracks_limit(mock_library, limit=50)

    # Assert
    mock_library.searchTracks.assert_called_once_with(limit=50)
    assert tracks == mock_tracks
    assert library_size == len(mock_tracks)

def test_get_all_tracks_limit_failure():
    # Arrange
    mock_library = MagicMock()
    mock_library.searchTracks.side_effect = Exception("Error retrieving tracks")

    # Act & Assert
    with pytest.raises(SystemExit):
        get_all_tracks_limit(mock_library, limit=50)

def test_get_all_tracks_limit_empty():
    # Arrange
    mock_library = MagicMock()
    mock_library.searchTracks.return_value = []

    # Act
    tracks, library_size = get_all_tracks_limit(mock_library, limit=50)

    # Assert
    mock_library.searchTracks.assert_called_once_with(limit=50)
    assert tracks == []
    assert library_size == 0


def test_extract_track_data_success():
    # Arrange
    mock_track = MagicMock()
    mock_track.title = "Test Title"
    mock_track.artist().title = "Test Artist"
    mock_track.album().title = "Test Album"
    mock_track.genres = [MagicMock(tag="Rock"), MagicMock(tag="Pop")]
    mock_track.addedAt.strftime.return_value = "2023-01-01"
    mock_track.media = [MagicMock(parts=[MagicMock(file="/path/to/file.mp3")])]
    mock_track.locations = ["/path/to/file.mp3"]
    mock_track.ratingKey = 12345
    server_name = "TestServer"
    filepath_prefix = "/path/to/"

    # Act
    result = extract_track_data(mock_track, server_name, filepath_prefix)

    # Assert
    expected_result = {
        'title': "Test Title",
        'artist': "Test Artist",
        'album': "Test Album",
        'genre': ["Rock", "Pop"],
        'added_date': "2023-01-01",
        'filepath': "/path/to/file.mp3",
        'location': "file.mp3",
        'TestServer_id': 12345
    }
    assert result == expected_result

def test_extract_track_data_no_genres():
    # Arrange
    mock_track = MagicMock()
    mock_track.title = "Test Title"
    mock_track.artist().title = "Test Artist"
    mock_track.album().title = "Test Album"
    mock_track.genres = []
    mock_track.addedAt.strftime.return_value = "2023-01-01"
    mock_track.media = [MagicMock(parts=[MagicMock(file="/path/to/file.mp3")])]
    mock_track.locations = ["/path/to/file.mp3"]
    mock_track.ratingKey = 12345
    server_name = "TestServer"
    filepath_prefix = "/path/to/"

    # Act
    result = extract_track_data(mock_track, server_name, filepath_prefix)

    # Assert
    expected_result = {
        'title': "Test Title",
        'artist': "Test Artist",
        'album': "Test Album",
        'genre': [],
        'added_date': "2023-01-01",
        'filepath': "/path/to/file.mp3",
        'location': "file.mp3",
        'TestServer_id': 12345
    }
    assert result == expected_result

def test_extract_track_data_no_media_parts():
    # Arrange
    mock_track = MagicMock()
    mock_track.title = "Test Title"
    mock_track.artist().title = "Test Artist"
    mock_track.album().title = "Test Album"
    mock_track.genres = [MagicMock(tag="Rock")]
    mock_track.addedAt.strftime.return_value = "2023-01-01"
    mock_track.media = []
    mock_track.locations = ["/path/to/file.mp3"]
    mock_track.ratingKey = 12345
    server_name = "TestServer"
    filepath_prefix = "/path/to/"

    # Act
    result = extract_track_data(mock_track, server_name, filepath_prefix)

    # Assert
    expected_result = {
        'title': "Test Title",
        'artist': "Test Artist",
        'album': "Test Album",
        'genre': ["Rock"],
        'added_date': "2023-01-01",
        'filepath': None,
        'location': "file.mp3",
        'TestServer_id': 12345
    }
    assert result == expected_result


def test_listify_track_data_success():
    # Arrange
    mock_tracks = [MagicMock(), MagicMock()]
    server_name = "TestServer"
    filepath_prefix = "/path/to/"
    expected_data = [extract_track_data(track, server_name, filepath_prefix) for track in mock_tracks]

    # Act
    result = listify_track_data(mock_tracks, server_name, filepath_prefix)

    # Assert
    assert result == expected_data

def test_listify_track_data_empty():
    # Arrange
    mock_tracks = []
    server_name = "TestServer"
    filepath_prefix = "/path/to/"

    # Act
    result = listify_track_data(mock_tracks, server_name, filepath_prefix)

    # Assert
    assert result == []

def test_listify_track_data_empty():
    # Arrange
    mock_tracks = []
    server_name = "TestServer"

    # Act
    result = listify_track_data(mock_tracks, server_name, 'foo')

    # Assert
    assert result == []

# def test_listify_track_data_partial_failure():
#     # Arrange
#     mock_tracks = [MagicMock(), None, MagicMock()]
#     server_name = "TestServer"
#     filepath_prefix = "/path/to/"
#     expected_data = [extract_track_data(track, server_name, filepath_prefix) for track in mock_tracks if track is not None]
#
#     # Act
#     result = listify_track_data(mock_tracks, server_name, filepath_prefix)
#
#     # Assert
#     assert result == expected_data

def test_export_track_data_success(tmp_path):
    # Arrange
    track_data = [
        {'title': 'Track1', 'artist': 'Artist1', 'album': 'Album1', 'genre': ['Rock'], 'added_date': '2023-01-01', 'filepath': '/path/to/file1.mp3', 'location': 'file1.mp3', 'TestServer_id': 1},
        {'title': 'Track2', 'artist': 'Artist2', 'album': 'Album2', 'genre': ['Pop'], 'added_date': '2023-01-02', 'filepath': '/path/to/file2.mp3', 'location': 'file2.mp3', 'TestServer_id': 2}
    ]
    filename = tmp_path / "output.csv"
    server_name = "TestServer"

    # Act
    export_track_data(track_data, filename, server_name)

    # Assert
    with open(filename, 'r') as f:
        lines = f.readlines()
    assert len(lines) == 3  # header + 2 data lines

def test_export_track_data_empty(tmp_path):
    # Arrange
    track_data = []
    filename = tmp_path / "output.csv"
    server_name = "TestServer"

    # Act
    export_track_data(track_data, filename, server_name)

    # Assert
    with open(filename, 'r') as f:
        lines = f.readlines()
    assert len(lines) == 1  # only header

def test_export_track_data_partial_failure(tmp_path):
    # Arrange
    track_data = [
        {'title': 'Track1', 'artist': 'Artist1', 'album': 'Album1', 'genre': ['Rock'], 'added_date': '2023-01-01', 'filepath': '/path/to/file1.mp3', 'location': 'file1.mp3', 'TestServer_id': 1},
        None,
        {'title': 'Track2', 'artist': 'Artist2', 'album': 'Album2', 'genre': ['Pop'], 'added_date': '2023-01-02', 'filepath': '/path/to/file2.mp3', 'location': 'file2.mp3', 'TestServer_id': 2}
    ]
    filename = tmp_path / "output.csv"
    server_name = "TestServer"

    # Act
    export_track_data([data for data in track_data if data is not None], filename, server_name)

    # Assert
    with open(filename, 'r') as f:
        lines = f.readlines()
    assert len(lines) == 3  # header + 2 data lines