import pandas as pd
import numpy as np
import time
import json
from typing import Dict, List, Tuple, Optional
from loguru import logger
# TODO: Complete import when integrating with lastfm module
# from analysis.lastfm import get_artist_info, get_artist_mbid, get_artist_tags, etc.
import analysis.bpm as bpm


# WIP - Pandas-based ETL pipeline scaffold
# Not fully integrated yet - placeholder methods need implementation
class MusicLibraryTransform:
    def __init__(self, csv_path: str, api_delay: float = 1.0):
        """
        Initialize the transformation pipeline

        Args:
            csv_path: Path to the CSV file exported from Plex
            api_delay: Delay in seconds between API calls for rate limiting
        """
        self.csv_path = csv_path
        self.api_delay = api_delay
        self.track_data = None
        self.artists = None
        self.genres = None
        self.artist_genres = None
        self.track_genres = None
        self.similar_artists = None
        self.exception_tracks = None
        self.exception_artists = None

        # Cache for API calls to avoid duplicates
        self.lastfm_artist_cache = {}
        self.lastfm_track_cache = {}

    def load_csv(self) -> pd.DataFrame:
        """Load the initial CSV data"""
        logger.info(f"Loading CSV from {self.csv_path}")
        self.track_data = pd.read_csv(self.csv_path)
        logger.info(f"Loaded {len(self.track_data)} tracks from CSV")
        return self.track_data

    # def clean_track_data(self) -> pd.DataFrame:
    #     """Perform basic cleaning on track data"""
    #     logger.info("Cleaning track data")
    #     # Basic cleaning operations like handling NaNs, etc.
    #     # Add any specific cleaning logic you need
    #
    #     # Example: Convert timestamps, handle missing values
    #     if 'duration' in self.track_data.columns:
    #         self.track_data['duration_sec'] = self.track_data['duration'].fillna(0).astype(int)
    #
    #     logger.info("Track data cleaning complete")
    #     return self.track_data

    def extract_artists(self) -> pd.DataFrame:
        """Extract unique artists and create artists DataFrame"""
        logger.info("Extracting unique artists")

        # Get unique artists
        artists_series = self.track_data['artist'].dropna().unique()

        # Create artists DataFrame with unique IDs
        self.artists = pd.DataFrame({
            'id': range(1, len(artists_series) + 1),
            'name': artists_series,
            'musicbrainz_id': np.nan
        })

        logger.info(f"Extracted {len(self.artists)} unique artists")

        # Add artist_id back to track_data
        self.track_data = self.track_data.merge(
            self.artists[['id', 'name']],
            left_on='artist',
            right_on='name',
            how='left'
        )
        self.track_data.rename(columns={'id': 'artist_id'}, inplace=True)
        self.track_data.drop('name', axis=1, inplace=True)

        return self.artists

    def process_various_artists(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process tracks with 'Various Artists' and update both tables"""
        logger.info("Processing 'Various Artists' tracks")

        # Find tracks with 'Various Artists'
        various_artists_mask = self.track_data['artist'] == 'Various Artists'
        various_tracks = self.track_data[various_artists_mask].copy()

        # Process each track to get actual artist
        updated_tracks = []
        new_artists = []
        exception_tracks = []

        next_artist_id = self.artists['id'].max() + 1

        for idx, track in various_tracks.iterrows():
            try:
                # Use your ffmpeg function to get actual artist info
                # This is a placeholder for your actual implementation
                file_path = track['file_path']
                ffmpeg_info = self.get_ffmpeg_info(file_path)
                artist_name, artist_mbid = self.extract_artist_from_ffmpeg(ffmpeg_info)

                if artist_name:
                    # Check if artist already exists
                    existing_artist = self.artists[self.artists['name'] == artist_name]

                    if len(existing_artist) > 0:
                        # Artist exists, use existing id
                        artist_id = existing_artist.iloc[0]['id']

                        # Update mbid if we have it and existing doesn't
                        if pd.isna(existing_artist.iloc[0]['musicbrainz_id']) and artist_mbid:
                            self.artists.loc[self.artists['name'] == artist_name, 'musicbrainz_id'] = artist_mbid
                    else:
                        # New artist
                        artist_id = next_artist_id
                        next_artist_id += 1
                        new_artists.append({
                            'id': artist_id,
                            'name': artist_name,
                            'musicbrainz_id': artist_mbid
                        })

                    # Update track info
                    track['artist'] = artist_name
                    track['artist_id'] = artist_id
                    updated_tracks.append(track)
                else:
                    # Couldn't get artist info, add to exceptions
                    exception_tracks.append({
                        'track_id': track.get('id', idx),
                        'title': track.get('title', 'Unknown'),
                        'reason': 'Could not extract artist from Various Artists track'
                    })
            except Exception as e:
                logger.error(f"Error processing Various Artists track {track.get('title', 'Unknown')}: {str(e)}")
                exception_tracks.append({
                    'track_id': track.get('id', idx),
                    'title': track.get('title', 'Unknown'),
                    'reason': f"Error: {str(e)}"
                })

            # Rate limiting
            time.sleep(self.api_delay)

        # Update track_data with new artist info
        if updated_tracks:
            updated_df = pd.DataFrame(updated_tracks)
            for idx, row in updated_df.iterrows():
                self.track_data.loc[self.track_data.index == row.name] = row

        # Add new artists to artists DataFrame
        if new_artists:
            new_artists_df = pd.DataFrame(new_artists)
            self.artists = pd.concat([self.artists, new_artists_df], ignore_index=True)

        # Create exception_tracks DataFrame if needed
        if exception_tracks:
            self.exception_tracks = pd.DataFrame(exception_tracks)

        logger.info(f"Updated {len(updated_tracks)} tracks with actual artist info")
        logger.info(f"Added {len(new_artists)} new artists")

        return self.track_data, self.artists

    def enrich_track_musicbrainz_ids(self) -> pd.DataFrame:
        """Enrich tracks with MusicBrainz IDs"""
        logger.info("Enriching tracks with MusicBrainz IDs")

        # Initialize new column if it doesn't exist
        if 'musicbrainz_id' not in self.track_data.columns:
            self.track_data['musicbrainz_id'] = np.nan

        tracks_to_process = self.track_data[pd.isna(self.track_data['musicbrainz_id'])].copy()
        logger.info(f"Processing {len(tracks_to_process)} tracks without MusicBrainz IDs")

        for idx, track in tracks_to_process.iterrows():
            try:
                # Try ffmpeg method first
                mbid = None
                file_path = track['file_path']

                ffmpeg_info = self.get_ffmpeg_info(file_path)
                mbid = self.extract_mbid_from_ffmpeg(ffmpeg_info)

                # If ffmpeg failed, try lastfm
                if not mbid:
                    track_name = track['title']
                    artist_name = track['artist']

                    # Use cache if available to avoid duplicate API calls
                    cache_key = f"{artist_name}|{track_name}"
                    if cache_key in self.lastfm_track_cache:
                        lastfm_data = self.lastfm_track_cache[cache_key]
                    else:
                        lastfm_data = self.get_lastfm_track_data(artist_name, track_name)
                        self.lastfm_track_cache[cache_key] = lastfm_data
                        time.sleep(self.api_delay)  # Rate limiting

                    mbid = self.extract_mbid_from_lastfm(lastfm_data)

                # Update the track
                if mbid:
                    self.track_data.loc[idx, 'musicbrainz_id'] = mbid
            except Exception as e:
                logger.error(f"Error enriching MusicBrainz ID for track {track.get('title', 'Unknown')}: {str(e)}")

        logger.info(
            f"Added MusicBrainz IDs to {len(self.track_data[~pd.isna(self.track_data['musicbrainz_id'])])} tracks")
        return self.track_data

    def enrich_artist_musicbrainz_ids(self) -> pd.DataFrame:
        """Enrich artists with MusicBrainz IDs"""
        logger.info("Enriching artists with MusicBrainz IDs")

        # Process artists without MusicBrainz IDs
        artists_to_process = self.artists[pd.isna(self.artists['musicbrainz_id'])].copy()
        logger.info(f"Processing {len(artists_to_process)} artists without MusicBrainz IDs")

        for idx, artist in artists_to_process.iterrows():
            try:
                artist_name = artist['name']
                mbid = None

                # Try using track data first
                tracks_by_artist = self.track_data[self.track_data['artist_id'] == artist['id']]

                if not tracks_by_artist.empty:
                    # Try to extract from a track's ffmpeg data
                    for _, track in tracks_by_artist.iterrows():
                        file_path = track['file_path']
                        ffmpeg_info = self.get_ffmpeg_info(file_path)
                        _, artist_mbid = self.extract_artist_from_ffmpeg(ffmpeg_info)

                        if artist_mbid:
                            mbid = artist_mbid
                            break

                # If we still don't have mbid, try lastfm
                if not mbid:
                    # Use cache if available
                    if artist_name in self.lastfm_artist_cache:
                        lastfm_data = self.lastfm_artist_cache[artist_name]
                    else:
                        lastfm_data = self.get_lastfm_artist_info(artist_name)
                        self.lastfm_artist_cache[artist_name] = lastfm_data
                        time.sleep(self.api_delay)  # Rate limiting

                    mbid = self.extract_artist_mbid_from_lastfm(lastfm_data)

                # Update the artist
                if mbid:
                    self.artists.loc[idx, 'musicbrainz_id'] = mbid
            except Exception as e:
                logger.error(f"Error enriching MusicBrainz ID for artist {artist['name']}: {str(e)}")

        logger.info(f"Added MusicBrainz IDs to {len(self.artists[~pd.isna(self.artists['musicbrainz_id'])])} artists")
        return self.artists

    def extract_genres(self) -> pd.DataFrame:
        """Extract genres from track data and LastFM tags"""
        logger.info("Extracting genres from track data")

        # Initialize genres set to avoid duplicates
        all_genres = set()

        # Process track genres from the CSV
        for idx, track in self.track_data.iterrows():
            if 'genre' in track and pd.notna(track['genre']):
                # Parse the genre string that looks like a Python list
                genre_str = track['genre']
                # Remove brackets and split
                if genre_str.startswith('[') and genre_str.endswith(']'):
                    genre_str = genre_str[1:-1]
                    # Split by comma, handle quotes
                    genres = [g.strip().strip("'\"") for g in genre_str.split(',')]
                    all_genres.update(genres)

        # Process LastFM artist tags (assuming we've already cached artist info)
        for artist_name, lastfm_data in self.lastfm_artist_cache.items():
            artist_tags = self.extract_tags_from_lastfm(lastfm_data)
            all_genres.update(artist_tags)

        # Create genres DataFrame
        self.genres = pd.DataFrame({
            'id': range(1, len(all_genres) + 1),
            'name': list(all_genres)
        })

        logger.info(f"Extracted {len(self.genres)} unique genres")
        return self.genres

    def create_artist_genres_table(self) -> pd.DataFrame:
        """Create artist_genres association table"""
        logger.info("Creating artist_genres association table")

        artist_genres_list = []

        # For each artist, get their genres from LastFM
        for idx, artist in self.artists.iterrows():
            artist_name = artist['name']

            # Skip if we don't have LastFM data
            if artist_name not in self.lastfm_artist_cache:
                continue

            lastfm_data = self.lastfm_artist_cache[artist_name]
            artist_tags = self.extract_tags_from_lastfm(lastfm_data)

            # For each tag, find the genre_id and create association
            for tag in artist_tags:
                genre_match = self.genres[self.genres['name'] == tag]
                if not genre_match.empty:
                    genre_id = genre_match.iloc[0]['id']
                    artist_genres_list.append({
                        'artist_id': artist['id'],
                        'genre_id': genre_id
                    })

        # Create artist_genres DataFrame
        self.artist_genres = pd.DataFrame(artist_genres_list).drop_duplicates()

        logger.info(f"Created {len(self.artist_genres)} artist-genre associations")
        return self.artist_genres

    def create_track_genres_table(self) -> pd.DataFrame:
        """Create track_genres association table"""
        logger.info("Creating track_genres association table")

        track_genres_list = []

        # For each track, get genres from its genre field
        for idx, track in self.track_data.iterrows():
            if 'genre' in track and pd.notna(track['genre']):
                # Parse the genre string
                genre_str = track['genre']
                if genre_str.startswith('[') and genre_str.endswith(']'):
                    genre_str = genre_str[1:-1]
                    genres = [g.strip().strip("'\"") for g in genre_str.split(',')]

                    # For each genre, find the genre_id and create association
                    for genre_name in genres:
                        genre_match = self.genres[self.genres['name'] == genre_name]
                        if not genre_match.empty:
                            genre_id = genre_match.iloc[0]['id']
                            track_genres_list.append({
                                'track_id': track.get('id', idx),
                                'genre_id': genre_id
                            })

        # Create track_genres DataFrame
        self.track_genres = pd.DataFrame(track_genres_list).drop_duplicates()

        logger.info(f"Created {len(self.track_genres)} track-genre associations")
        return self.track_genres

    def get_bpm_for_tracks(self) -> pd.DataFrame:
        """Add BPM information to tracks"""
        logger.info("Adding BPM information to tracks")

        # Initialize BPM column if needed
        if 'bpm' not in self.track_data.columns:
            self.track_data['bpm'] = np.nan

        # Process tracks without BPM
        tracks_to_process = self.track_data[pd.isna(self.track_data['bpm'])].copy()
        logger.info(f"Processing BPM for {len(tracks_to_process)} tracks")

        for idx, track in tracks_to_process.iterrows():
            try:
                file_path = track['file_path']
                is_m4a = file_path.lower().endswith('.m4a')

                if is_m4a:
                    # For m4a files, convert to wav first
                    wav_path = self.convert_m4a_to_wav(file_path)
                    bpm = self.get_bpm(wav_path)
                    self.cleanup_temp_file(wav_path)
                else:
                    # For other files, process directly
                    bpm = self.get_bpm(file_path)

                if bpm:
                    self.track_data.loc[idx, 'bpm'] = bpm
            except Exception as e:
                logger.error(f"Error getting BPM for track {track.get('title', 'Unknown')}: {str(e)}")

            # No need for rate limiting here as it's local processing

        logger.info(f"Added BPM to {len(self.track_data[~pd.isna(self.track_data['bpm'])])} tracks")
        return self.track_data

    def create_similar_artists_table(self) -> pd.DataFrame:
        """Create similar_artists association table"""
        logger.info("Creating similar_artists association table")

        similar_artists_list = []
        new_artists = []
        next_artist_id = self.artists['id'].max() + 1

        # For each artist, get similar artists from LastFM
        for idx, artist in self.artists.iterrows():
            artist_name = artist['name']
            artist_id = artist['id']

            # Skip if we don't have LastFM data
            if artist_name not in self.lastfm_artist_cache:
                continue

            lastfm_data = self.lastfm_artist_cache[artist_name]
            similar_artist_names = self.extract_similar_artists_from_lastfm(lastfm_data)

            # For each similar artist, find or create artist and create association
            for similar_name in similar_artist_names:
                # Check if artist already exists
                similar_artist_match = self.artists[self.artists['name'] == similar_name]

                if not similar_artist_match.empty:
                    # Artist exists
                    similar_id = similar_artist_match.iloc[0]['id']
                else:
                    # New artist
                    similar_id = next_artist_id
                    next_artist_id += 1
                    new_artists.append({
                        'id': similar_id,
                        'name': similar_name,
                        'musicbrainz_id': np.nan
                    })

                # Create association (in both directions for bidirectional relationship)
                similar_artists_list.append({
                    'artist_id': artist_id,
                    'similar_artist_id': similar_id
                })
                # If you want a bidirectional relationship, uncomment:
                # similar_artists_list.append({
                #     'artist_id': similar_id,
                #     'similar_artist_id': artist_id
                # })

        # Add new artists if any
        if new_artists:
            new_artists_df = pd.DataFrame(new_artists)
            self.artists = pd.concat([self.artists, new_artists_df], ignore_index=True)

        # Create similar_artists DataFrame
        self.similar_artists = pd.DataFrame(similar_artists_list).drop_duplicates()

        logger.info(f"Created {len(self.similar_artists)} similar-artist associations")
        logger.info(f"Added {len(new_artists)} new artists from similar artists")

        return self.similar_artists

    def save_checkpoint(self, prefix: str = 'checkpoint') -> None:
        """Save current state of all DataFrames to CSV files"""
        logger.info(f"Saving checkpoint with prefix '{prefix}'")

        if self.track_data is not None:
            self.track_data.to_csv(f"{prefix}_track_data.csv", index=False)

        if self.artists is not None:
            self.artists.to_csv(f"{prefix}_artists.csv", index=False)

        if self.genres is not None:
            self.genres.to_csv(f"{prefix}_genres.csv", index=False)

        if self.artist_genres is not None:
            self.artist_genres.to_csv(f"{prefix}_artist_genres.csv", index=False)

        if self.track_genres is not None:
            self.track_genres.to_csv(f"{prefix}_track_genres.csv", index=False)

        if self.similar_artists is not None:
            self.similar_artists.to_csv(f"{prefix}_similar_artists.csv", index=False)

        if self.exception_tracks is not None:
            self.exception_tracks.to_csv(f"{prefix}_exception_tracks.csv", index=False)

        if self.exception_artists is not None:
            self.exception_artists.to_csv(f"{prefix}_exception_artists.csv", index=False)

        logger.info("Checkpoint saved successfully")

    def load_checkpoint(self, prefix: str = 'checkpoint') -> None:
        """Load state from checkpoint CSV files"""
        logger.info(f"Loading checkpoint with prefix '{prefix}'")

        try:
            self.track_data = pd.read_csv(f"{prefix}_track_data.csv")
            self.artists = pd.read_csv(f"{prefix}_artists.csv")
            self.genres = pd.read_csv(f"{prefix}_genres.csv")
            self.artist_genres = pd.read_csv(f"{prefix}_artist_genres.csv")
            self.track_genres = pd.read_csv(f"{prefix}_track_genres.csv")
            self.similar_artists = pd.read_csv(f"{prefix}_similar_artists.csv")

            # These might not exist
            try:
                self.exception_tracks = pd.read_csv(f"{prefix}_exception_tracks.csv")
            except:
                pass

            try:
                self.exception_artists = pd.read_csv(f"{prefix}_exception_artists.csv")
            except:
                pass

            logger.info("Checkpoint loaded successfully")
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            raise

    # Placeholder methods for external API calls and processing - these would be implemented with your actual functions

    def get_ffmpeg_info(self, file_path: str) -> dict:
        """Get ffmpeg info from file path"""
        # Placeholder - replace with your implementation
        return {}

    def extract_artist_from_ffmpeg(self, ffmpeg_info: dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract artist name and MBID from ffmpeg info"""
        # Placeholder - replace with your implementation
        return None, None

    def extract_mbid_from_ffmpeg(self, ffmpeg_info: dict) -> Optional[str]:
        """Extract MusicBrainz ID from ffmpeg info"""
        # Placeholder - replace with your implementation
        return None

    def get_lastfm_track_data(self, artist: str, track: str) -> dict:
        """Get LastFM data for a track"""
        # Placeholder - replace with your implementation
        return {}

    def extract_mbid_from_lastfm(self, lastfm_data: dict) -> Optional[str]:
        """Extract MusicBrainz ID from LastFM track data"""
        # Placeholder - replace with your implementation
        return None

    def get_lastfm_artist_info(self, artist: str) -> dict:
        """Get LastFM info for an artist"""
        # Placeholder - replace with your implementation
        return {}

    def extract_artist_mbid_from_lastfm(self, lastfm_data: dict) -> Optional[str]:
        """Extract artist MusicBrainz ID from LastFM data"""
        # Placeholder - replace with your implementation
        return None

    def extract_tags_from_lastfm(self, lastfm_data: dict) -> List[str]:
        """Extract tags from LastFM data"""
        # Placeholder - replace with your implementation
        return []

    def extract_similar_artists_from_lastfm(self, lastfm_data: dict) -> List[str]:
        """Extract similar artists from LastFM data"""
        # Placeholder - replace with your implementation
        return []

    def convert_m4a_to_wav(self, file_path: str) -> str:
        """Convert m4a to wav for BPM analysis"""
        # Placeholder - replace with your implementation
        return ""

    def get_bpm(self, file_path: str) -> Optional[float]:
        """Get BPM from audio file using librosa"""
        # Placeholder - replace with your implementation
        return None

    def cleanup_temp_file(self, file_path: str) -> None:
        """Clean up temporary file"""
        # Placeholder - replace with your implementation
        pass

    def run_full_pipeline(self) -> Dict[str, pd.DataFrame]:
        """Run the complete transformation pipeline"""
        logger.info("Starting full transformation pipeline")

        # Load and clean data
        self.load_csv()
        self.clean_track_data()
        self.save_checkpoint('after_clean')

        # Process artists
        self.extract_artists()
        self.process_various_artists()
        self.save_checkpoint('after_artists')

        # Enrich with MusicBrainz IDs
        self.enrich_track_musicbrainz_ids()
        self.enrich_artist_musicbrainz_ids()
        self.save_checkpoint('after_mbids')

        # Process genres
        self.extract_genres()
        self.create_artist_genres_table()
        self.create_track_genres_table()
        self.save_checkpoint('after_genres')

        # Add BPM data
        self.get_bpm_for_tracks()
        self.save_checkpoint('after_bpm')

        # Create similar artists
        self.create_similar_artists_table()
        self.save_checkpoint('final')

        logger.info("Transformation pipeline complete")

        # Return all DataFrames
        return {
            'track_data': self.track_data,
            'artists': self.artists,
            'genres': self.genres,
            'artist_genres': self.artist_genres,
            'track_genres': self.track_genres,
            'similar_artists': self.similar_artists,
            'exception_tracks': self.exception_tracks,
            'exception_artists': self.exception_artists,
        }