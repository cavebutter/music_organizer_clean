from plex.plex_library import get_music_library

# Overview of the Initial Load and Analysis Workflow  
This document outlines the steps involved in the initial load and analysis workflow for a data pipeline. The workflow consists of several key stages, including data extraction, transformation, loading, and analysis. Each stage is designed to ensure that the data is processed efficiently and accurately.
## Workflow Steps
1. **Data Extraction**: Track data is extracted from Plex server and exported to csv with minimal transformation.
2. **Data Transformation in Pandas**: Pandas consumes the csv and performs transformation, analysis, and creation of additional and supporting tables.  Described in Transformation section below.
3. **Data Loading to MySQL**: The transformed data is loaded into a MySQL database.

## Data Extraction
`plex.plex_library` is a module that contains functions for interacting with the Plex server.  A pipline of the following 
functions is used to extract data from the Plex server and export it to a CSV file:
```python
import plex.plex_library as pl
server = pl.plex_connect()
library = pl.get_music_library(server)
tracks, lib_size = pl.get_all_tracks_limit(library)
track_list = pl.listify_track_data(tracks,'file/path/prefix')
pl.export_track_data(track_list, 'output/track_data.csv')
```
## Data Transformation and Analysis in Pandas
The data transformation process is performed using the Pandas library, which provides powerful data manipulation and analysis capabilities. The following steps outline the key transformations applied to the extracted data:
1. **track_data table**: The csv data is loaded into a df that will become the track_data table.  Plex data is generally well-structured, and requires minimal cleansing.  
2. **artists table**: The artist data is extracted from the track_data table and loaded into a new table.
3. **track_data.artist_id**: This column will be added to the track_data table.  It is a foreign key to the artist table.
4. **track_data.artist**: Where a track is listed as 'Various Artists', use `analysis.ffmpeg.ffmpeg_get_info()` -> `analysis.ffmpeg.ffmpeg_get_track_artist_and_artist_mbid()` to try for a better match.  If there is a better match, create an entry in artists table with artist name and mbid and update `track_data.artist_id` and `track_data.artist` with the new data.
5. **track_data.musicbrainz_id**: This column will be added to the track_data table.  There are two ways to source this data:   
    a. `analysis.ffmpeg.ffmpeg_get_info()` -> `analysis.ffmpeg.ffmpeg_get_mbtid()`  This works well when the track has good metadata.  
    b. If this fails, then we use `analysis.lastfm.get_last_fm_track_data()` -> `analysis.lastfm.get_track_mbid()`. The latter is less reliable.  If neither method works, can be Null.  
6. **artists.musicbrainz_id**: This column will be added to the artists table.  It is populated in one of two ways:  
    a. `analysis.ffmpeg.ffmpeg_get_info()` -> `analysis.ffmpeg.ffmpeg_get_track_artist_and_artist_mbid()`  This works well when the track has good metadata.  If the artist does not exist in the artists table, create one with this data. If the artist exists, but without mbid, update the arttist record with the mbid.
    b. If this fails, then we use `analysis.lastfm.get_arttist_info()` -> `analysis.lastfm.get_artist_mbid()`. The latter is less reliable.  If neither method works, can be Null.
7. **track_data.bpm**: This column will be added to the track_data table. Beats Per Minute is obtained in one of two methods:  
    a. If the file is `.m4a`, we use `analysis.ffmpeg.convert_m4a_to_wav()` -> `analysis.bpm.get_bpm()` -> `analysis.ffmpeg.cleanup_temp_file()`.  
    b. For all other filetypes, we use `analysis.bpm.get_bpm()`.
8. **genres**: The genre data is extracted from the `track_data` table and loaded into a new table with `id` and `genre` columns.  This table is populated in the following ways:  
    a. `track_data.genre` is a string that was once a Python list.  The string is prepended with a `[` and appended with a `]`.  Each item is enclosed in single quotes and the items are separated by commas.  The brackets and quotes must be stripped and each item, if not in the `genres` table, must be added.  
    b. We use `analysis.lastfm.get_artist_info()` -> `analysis.lastfm.get_artist_tags()` to retrieve the top tags for artists.  These tags are added to the `genres` table if not already present.
9. **artist_genres**: This table is a many-to-many relationship between artists and genres.  It is populated by iterating through the `artists` and `genres` tables and creating a record for each artist/genre combination. The associations are created from the results of 
`analysis.lastfm.get_artist_info()` -> `analysis.lastfm.get_artist_tags()`.
10. **track_genres**: This table is a many-to-many relationship between tracks and genres.  It is populated by two methods:  
    a. iterating through the `track_data` and `genres` tables and creating a record for each track/genre combination.  
    b. `analysis.lastfm.get_last_fm_track_data()` -> `analysis.lastfm.get_track_tags()` to retrieve the top tags for tracks.  If a tag is not in the `genres` table, it is added.  A record is created for each track/tag combination.
11. **similar_artists**: This table is a many-to-many relationship between artists and artists.  It is populated by iterating through the `artists` table and using `analysis.lastfm.get_artist_info()` -> `analysis.lastfm.get_similar_artists()` to retrieve similar artists.  Each similar artist is added to the `artists` table if not already present, and a record is created in the `similar_artists` table for each artist/similar artist combination.
We find similar artists with `analysis.lastfm.get_artist_info()` -> `analysis.lastfm.get_similar_artists()`.  When a similar artist is not in `artists`, we add it, but do not need to analyze it for mbid or genre.

## Data Loading to MySQL
The transformed data is loaded into a MySQL database using the `sqlalchemy` library. The following steps outline the key loading processes:
1. **Database Connection**: A connection to the MySQL database is established using the `sqlalchemy` library.
2. **Table Creation**: The necessary tables are created in the MySQL database if they do not already exist.
3. **Data Insertion**: The transformed data is inserted into the corresponding tables in the MySQL database.
4. **Data Validation**: The data is validated to ensure that it has been loaded correctly and is consistent with the original data.
5. **Data Backup**: A backup of the MySQL database is created to ensure data integrity and recovery in case of any issues.
## Conclusion
The initial load and analysis workflow is a critical process for ensuring that data is extracted, transformed, and loaded accurately and efficiently. By following the steps outlined in this document, organizations can ensure that their data pipeline is robust and reliable, enabling them to make informed decisions based on accurate data.

## Important Notes
The functions referenced in the Data Transformation and  Analysis In Pandas section are developed and testedd.  However, 
the pipeline has not been implemented for Pandas.  Careful consideration should be given to the order of execution to ensure efficiency in calls to external APIs and to maintain data integrity.
