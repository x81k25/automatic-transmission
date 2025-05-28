from datetime import datetime, timezone
from enum import Enum
import polars as pl
from typing import Any, Optional

# ------------------------------------------------------------------------------
# enum classes
# ------------------------------------------------------------------------------

pl.enable_string_cache()

class PipelineStatus(str, Enum):
    INGESTED = 'ingested'
    PAUSED = 'paused'
    PARSED = 'parsed'
    FILE_ACCEPTED = 'file_accepted'
    METADATA_COLLECTED = 'metadata_collected'
    MEDIA_ACCEPTED = 'media_accepted'
    DOWNLOADING = 'downloading'
    DOWNLOADED = 'downloaded'
    TRANSFERRED = 'transferred'
    COMPLETE = 'complete'
    REJECTED = 'rejected'


class RejectionStatus(str, Enum):
    UNFILTERED = 'unfiltered'
    ACCEPTED = 'accepted'
    REJECTED = 'rejected'
    OVERRIDE = 'override'


class MediaType(str, Enum):
    MOVIE = 'movie'
    TV_SHOW = 'tv_show'
    TV_SEASON = 'tv_season'
    UNKNOWN = 'unknown'


class RssSource(str, Enum):
    YTS = 'yts.mx'
    EPISODE_FEED = 'episodefeed.com'


# ------------------------------------------------------------------------------
# unified media dataframe class
# ------------------------------------------------------------------------------

class MediaDataFrame:
    """Unified rigid polars DataFrame for all media types matching SQL schemas."""

    # Complete schema with all possible fields
    schema = {
        # identifier column
        'hash': pl.Utf8,
        # media information
        'media_type': pl.Categorical,
        'media_title': pl.Utf8,
        'season': pl.Int64,
        'episode': pl.Int64,
        'release_year': pl.Int64,
        # pipeline status information
        'pipeline_status': pl.Categorical,
        'error_status': pl.Boolean,
        'error_condition': pl.Utf8,
        'rejection_status': pl.Categorical,
        'rejection_reason': pl.Utf8,
        # path information
        'parent_path': pl.Utf8,
        'target_path': pl.Utf8,
        # download information
        'original_title': pl.Utf8,
        'original_path': pl.Utf8,
        'original_link': pl.Utf8,
        'rss_source': pl.Categorical,
        'uploader': pl.Utf8,
        # metadata pertaining to the media item
        # - identifier fields
        'imdb_id': pl.Utf8,
        'tmdb_id': pl.Int64,
        # - quantitative fields
        'budget': pl.Int64,
        'revenue': pl.Int64,
        'runtime': pl.Int64,
        # - country and production information
        'origin_country': pl.List(pl.Utf8),
        'production_companies': pl.List(pl.Utf8),
        'production_countries': pl.List(pl.Utf8),
        'production_status': pl.Utf8,
        # - language information
        'original_language': pl.Utf8,
        'spoken_languages': pl.List(pl.Utf8),
        # - other string fields
        'genre': pl.List(pl.Utf8),
        'original_media_title': pl.Utf8,
        # - long string fields
        'tagline': pl.Utf8,
        'overview': pl.Utf8,
        # - ratings info
        'tmdb_rating': pl.Float64,
        'tmdb_votes': pl.Int64,
        'rt_score': pl.Int64,
        'metascore': pl.Int64,
        'imdb_rating': pl.Float64,
        'imdb_votes': pl.Int64,
        # metadata pertaining to the video file
        'resolution': pl.Utf8,
        'video_codec': pl.Utf8,
        'upload_type': pl.Utf8,
        'audio_codec': pl.Utf8,
        # timestamps
        'created_at': pl.Datetime,
        'updated_at': pl.Datetime,
    }

    # Common required columns
    required_columns = [
        'hash',
        'original_title',
        'media_type',
        'pipeline_status',
        'error_status',
        'rejection_status'
    ]


    def __init__(self, data: Optional[Any] = None):
        """
        Initialize with data that can be converted to a polars DataFrame.

        :param data: Data to convert to DataFrame
        """
        if data is None:
            # Create empty DataFrame with proper schema
            self._df = pl.DataFrame(schema=self.schema)
        elif isinstance(data, pl.DataFrame):
            self._validate_and_prepare(data)
        else:
            # Try to create from other data types (dict, list, etc.)
            try:
                # Use the predefined schema here
                self._df = pl.DataFrame(data, schema=self.schema)
                self._validate_and_prepare(self._df)
            except Exception as e:
                raise ValueError(
                    f"Could not create MediaDataFrame from data: {e}")


    def _ensure_unique_hashes(self, df: pl.DataFrame) -> pl.DataFrame:
       """Check for duplicate hashes and raise error if found."""
       if df.height == 0:
           return df

       # Find duplicate hashes
       duplicates = df.filter(pl.col('hash').is_duplicated())

       if duplicates.height > 0:
           print("Duplicate hashes found:")
           for row in duplicates.iter_rows(named=True):
               print(f"Hash: {row['hash']}, Title: {row.get('original_title', 'N/A')}, Type: {row.get('media_type', 'N/A')}")

           raise ValueError(f"Found {duplicates.height} duplicate hash(es) in MediaDataFrame")

       return df


    def _validate_and_prepare(self, df: pl.DataFrame) -> None:
        """
        Validate that DataFrame conforms to the required schema and prepare it.
        """
        # Check required columns
        missing = [col for col in self.required_columns if
                   col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Ensure unique hashes
        df = self._ensure_unique_hashes(df)

        # Get current timestamp once to ensure consistency
        current_timestamp = datetime.now(timezone.utc)

        # For both timestamp columns, conditionally add or update
        timestamp_exprs = []

        # Handle created_at
        if 'created_at' in df.columns:
            # First check if the column has any non-null values
            if df['created_at'].null_count() < len(df):
                timestamp_exprs.append(
                    pl.when(pl.col('created_at').is_null())
                    .then(pl.lit(current_timestamp))
                    .otherwise(
                        # Only apply timezone conversion to non-null values
                        pl.when(pl.col('created_at').is_not_null())
                        .then(pl.col('created_at').dt.replace_time_zone('UTC'))
                        .otherwise(pl.lit(current_timestamp))
                    )
                    .alias('created_at')
                )
            else:
                # All values are null, just use current timestamp
                timestamp_exprs.append(
                    pl.lit(current_timestamp).alias('created_at'))
        else:
            timestamp_exprs.append(
                pl.lit(current_timestamp).alias('created_at'))

        # Handle updated_at - same logic
        if 'updated_at' in df.columns:
            if df['updated_at'].null_count() < len(df):
                timestamp_exprs.append(
                    pl.when(pl.col('updated_at').is_null())
                    .then(pl.lit(current_timestamp))
                    .otherwise(
                        pl.when(pl.col('updated_at').is_not_null())
                        .then(pl.col('updated_at').dt.replace_time_zone('UTC'))
                        .otherwise(pl.lit(current_timestamp))
                    )
                    .alias('updated_at')
                )
            else:
                timestamp_exprs.append(
                    pl.lit(current_timestamp).alias('updated_at'))
        else:
            timestamp_exprs.append(
                pl.lit(current_timestamp).alias('updated_at'))

        # Apply all expressions at once
        df = df.with_columns(timestamp_exprs)

        # Set the underlying DataFrame
        self._df = df


    def to_schema(self) -> 'MediaDataFrame':
       """
       return MediaDataFrame with only schema columns, validating types; this
           will primarily be leverage to prevent errors when committing
           database updates
       """
       try:
           # Filter to only schema columns - polars will raise if any are missing
           filtered_df = self._df.select(list(self.schema.keys()))
       except pl.ColumnNotFoundError as e:
           raise ValueError(f"Missing required schema columns: {str(e)}")

       # Check type compatibility and collect issues
       issues = []
       for col_name, expected_type in self.schema.items():
           actual_type = filtered_df[col_name].dtype

           # Check if types are compatible using polars type hierarchy
           try:
               # Create a dummy series with expected type to test compatibility
               pl.Series([None], dtype=expected_type).cast(actual_type)
               pl.Series([None], dtype=actual_type).cast(expected_type)
           except (pl.ComputeError, pl.InvalidOperationError):
               issues.append(
                   f"Column '{col_name}': expected {expected_type}-compatible type, got {actual_type}"
               )

       # Raise comprehensive error if any issues found
       if issues:
           issue_list = "\n- ".join([""] + issues)
           raise ValueError(f"Schema validation failed with {len(issues)} issue{'s' if len(issues) > 1 else ''}:{issue_list}")

       # Return new MediaDataFrame instance with filtered data
       return MediaDataFrame(filtered_df)


    def update(self, df: pl.DataFrame):
        """Update internal DataFrame directly."""
        self._validate_and_prepare(df)
        self._df = df


    def append(self, new_df: pl.DataFrame) -> None:
        """
        Append new rows to the existing DataFrame.

        :param new_df: DataFrame with new rows to append
        """
        if new_df.height == 0:
            return  # Nothing to append

        # Check required columns in new data
        missing = [col for col in self.required_columns if col not in new_df.columns]
        if missing:
            raise ValueError(f"Missing required columns in new data: {missing}")

        # Create null rows with proper schema
        null_data = {col_name: [None] * new_df.height for col_name in self.schema.keys()}
        empty_rows = pl.DataFrame(null_data, schema=self.schema)

        # Update with actual data from new_df, casting to match existing types
        update_exprs = []
        for col in new_df.columns:
            if col in self.schema:
                # Cast to the exact dtype from existing DataFrame
                existing_dtype = self._df[col].dtype
                update_exprs.append(new_df[col].cast(existing_dtype).alias(col))

        structured_df = empty_rows.with_columns(update_exprs)

        # Use vstack to combine
        combined_df = self._df.vstack(structured_df)
        self._validate_and_prepare(combined_df)


    def filter(self, *predicates) -> 'MediaDataFrame':
        """
        filter the MediaDataFrame using polars filter syntax

        :param *predicates: one or more filter expressions (same as polars DataFrame.filter)
        :return: MediaDataFrame: New filtered MediaDataFrame instance
        """
        filtered_df = self._df.filter(*predicates)
        return MediaDataFrame(filtered_df)


    @property
    def df(self) -> pl.DataFrame:
        """Access the underlying polars DataFrame."""
        return self._df

# ------------------------------------------------------------------------------
# end of data_models.py
# ------------------------------------------------------------------------------