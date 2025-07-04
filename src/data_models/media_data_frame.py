from enum import Enum
import polars as pl
from typing import Any, Optional

# -----------------------------------------------------------------------------
# enum classes
# -----------------------------------------------------------------------------

pl.enable_string_cache()

class PipelineStatus(str, Enum):
    INGESTED = 'ingested'
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


# -----------------------------------------------------------------------------
# unified media dataframe class
# -----------------------------------------------------------------------------

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
    }

    # common required columns
    required_columns = [
        'hash',
        'original_title',
        'media_type',
    ]

    # columns for which a default value will be applied if null
    default_values = {
        'pipeline_status': PipelineStatus.INGESTED,
        'rejection_status': RejectionStatus.UNFILTERED,
        'rejection_reason': None,
        'error_status': False,
        'error_condition': None
    }

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

    # -------------------------------------------------------------------------
    # validation functions
    # -------------------------------------------------------------------------

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

        # assign default values, if needed
        df = self._assign_default_values(df)

        # flip rejection flags
        df = self._flip_rejection_flags(df)

        # flip error flags
        df = self._flip_error_flags(df)

        # Set the underlying DataFrame
        self._df = df


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


    def _assign_default_values(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        use default values dictionary to assign default values to features
            which should have them

        :param df: DataFrame to check for default values
        :return: DataFrame with default values assigned
        """
        if df.height == 0:
            return df

        # add column if missing entirely
        missing_cols = [key for key in self.default_values.keys() if key not in df.columns]
        if missing_cols:
            df = df.with_columns([pl.lit(None).alias(col) for col in missing_cols])

        # one column existence is guaranteed, assign default value where needed
        expressions = [
            pl.when(pl.col(key).is_null())
                .then(pl.lit(value))
                .otherwise(pl.col(key))
                .alias(key)
            for key, value in self.default_values.items()
        ]

        df = df.with_columns(expressions)

        return df


    def _flip_rejection_flags(self, df):
        """
        applies logic to properly assign the rejection_status based off
            the existing rejection_status and the presence or absence of
            a rejection_reason

        :param df: DataFrame containing the proper rejection_reasons value,
            before the update is applied to rejection_status
        :return: DataFrame with correct values for rejection_status and
            rejection_reason
        """
        if df.height == 0:
            return df

        df = df.with_columns(
            rejection_status = pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
                .then(pl.lit(RejectionStatus.OVERRIDE))
            .otherwise(
                pl.when(~pl.col('rejection_reason').is_null())
                    .then(pl.lit(RejectionStatus.REJECTED))
                .otherwise(pl.col('rejection_status'))
            )
        )

        return df


    def _flip_error_flags(self, df):
        """
        applies logic to properly assign the error_status based off
            the presence or absence of an error_condition

        :param df: DataFrame containing the proper error_condition value,
            before the update is applied to error_status
        :return: DataFrame with correct values for error_condition and
            error_status
        """
        if df.height == 0:
            return df

        df = df.with_columns(
            error_status = pl.when(pl.col('error_condition').is_null())
                .then(pl.lit(False))
            .otherwise(pl.lit(True))
        )

        return df


    # -------------------------------------------------------------------------
    # mirrored vanilla polars functions
    # -------------------------------------------------------------------------

    @property
    def height(self) -> int:
        """Get the number of rows in the DataFrame."""
        return self._df.height


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


    def update(self, df: pl.DataFrame):
        """Update internal DataFrame directly."""
        self._validate_and_prepare(df)


    # -------------------------------------------------------------------------
    # other class functions
    # -------------------------------------------------------------------------

    @property
    def df(self) -> pl.DataFrame:
        """Access the underlying polars DataFrame."""
        return self._df


    def to_schema(self) -> 'MediaDataFrame':
        """
        return MediaDataFrame with only schema columns, validating types; this
           will primarily be leverage to prevent errors when committing
           database updates
        """
        # Add missing columns with appropriate null values
        existing_cols = set(self._df.columns)
        missing_cols = [col for col in self.schema.keys() if col not in existing_cols]

        df = self._df
        if missing_cols:
            null_exprs = [pl.lit(None, dtype=dtype).alias(col)
                         for col, dtype in self.schema.items()
                         if col in missing_cols]
            df = df.with_columns(null_exprs)  # Actually add the columns

        try:
            filtered_df = df.select(list(self.schema.keys()))  # Use df, not self._df
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


# ------------------------------------------------------------------------------
# end of data_models.py
# ------------------------------------------------------------------------------