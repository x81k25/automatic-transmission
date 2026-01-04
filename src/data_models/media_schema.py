"""
Pandera Polars schema for media data validation.

Schema based on PostgreSQL atp.media table:
https://github.com/x81k25/wiring-schematics/tree/main/docs/atp.media.md
"""
from enum import Enum
from typing import Optional, List
import pandera.polars as pa
import polars as pl

pl.enable_string_cache()


# -----------------------------------------------------------------------------
# Enum classes
# -----------------------------------------------------------------------------

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
    TV_EPISODE_PACK = 'tv_episode_pack'
    UNKNOWN = 'unknown'


class RssSource(str, Enum):
    YTS = 'yts.mx'
    EPISODE_FEED = 'episodefeed.com'


class LabelType(str, Enum):
    WOULD_WATCH = 'would_watch'
    WOULD_NOT_WATCH = 'would_not_watch'


# -----------------------------------------------------------------------------
# Schema constants
# -----------------------------------------------------------------------------

MEDIA_SCHEMA_COLUMNS = [
    'hash', 'media_type', 'media_title', 'season', 'episode', 'release_year',
    'pipeline_status', 'error_status', 'error_condition', 'rejection_status',
    'rejection_reason', 'parent_path', 'target_path', 'original_title',
    'original_path', 'original_link', 'rss_source', 'uploader', 'imdb_id',
    'tmdb_id', 'resolution', 'video_codec', 'upload_type', 'audio_codec'
]

POLARS_SCHEMA = {
    'hash': pl.Utf8,
    'media_type': pl.Categorical,
    'media_title': pl.Utf8,
    'season': pl.Int64,
    'episode': pl.Int64,
    'release_year': pl.Int64,
    'pipeline_status': pl.Categorical,
    'error_status': pl.Boolean,
    'error_condition': pl.Utf8,
    'rejection_status': pl.Categorical,
    'rejection_reason': pl.Utf8,
    'parent_path': pl.Utf8,
    'target_path': pl.Utf8,
    'original_title': pl.Utf8,
    'original_path': pl.Utf8,
    'original_link': pl.Utf8,
    'rss_source': pl.Categorical,
    'uploader': pl.Utf8,
    'imdb_id': pl.Utf8,
    'tmdb_id': pl.Int64,
    'resolution': pl.Utf8,
    'video_codec': pl.Utf8,
    'upload_type': pl.Utf8,
    'audio_codec': pl.Utf8,
}

DEFAULT_VALUES = {
    'pipeline_status': PipelineStatus.INGESTED.value,
    'rejection_status': RejectionStatus.UNFILTERED.value,
    'rejection_reason': None,
    'error_status': False,
    'error_condition': None
}


# -----------------------------------------------------------------------------
# Pandera Schema
# -----------------------------------------------------------------------------

class MediaSchema(pa.DataFrameModel):
    """Pandera schema for media data validation."""

    # Identifier (required)
    hash: str = pa.Field(nullable=False)

    # Media information
    media_type: str = pa.Field(nullable=False)
    media_title: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 255})
    season: Optional[int] = pa.Field(nullable=True)
    episode: Optional[int] = pa.Field(nullable=True)
    release_year: Optional[int] = pa.Field(nullable=True, ge=1900, le=2100)

    # Pipeline status
    pipeline_status: str = pa.Field(nullable=False)
    error_status: bool = pa.Field(nullable=False)
    error_condition: Optional[str] = pa.Field(nullable=True)
    rejection_status: str = pa.Field(nullable=False)
    rejection_reason: Optional[str] = pa.Field(nullable=True)

    # Paths
    parent_path: Optional[str] = pa.Field(nullable=True)
    target_path: Optional[str] = pa.Field(nullable=True)

    # Download info
    original_title: str = pa.Field(nullable=False)
    original_path: Optional[str] = pa.Field(nullable=True)
    original_link: Optional[str] = pa.Field(nullable=True)
    rss_source: Optional[str] = pa.Field(nullable=True)
    uploader: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 25})

    # External identifiers (for training table linkage)
    imdb_id: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 10})
    tmdb_id: Optional[int] = pa.Field(nullable=True)

    # File metadata
    resolution: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 10})
    video_codec: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 10})
    upload_type: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 10})
    audio_codec: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 10})

    class Config:
        coerce = True
        strict = False

    @classmethod
    def validate(cls, df: pl.DataFrame, lazy: bool = False) -> pl.DataFrame:
        """
        Validate and normalize DataFrame:
        1. Add all missing schema columns
        2. Assign default values for status fields
        3. Apply flag-flipping logic
        4. Check unique hashes
        5. Run Pandera validation
        6. Return columns in schema order
        """
        if df.height == 0:
            return df

        # Add all missing schema columns
        missing = [c for c in MEDIA_SCHEMA_COLUMNS if c not in df.columns]
        if missing:
            df = df.with_columns([
                pl.lit(None, dtype=POLARS_SCHEMA.get(c, pl.Utf8)).alias(c)
                for c in missing
            ])

        # Assign defaults where null
        df = df.with_columns([
            pl.when(pl.col(col).is_null())
                .then(pl.lit(val))
                .otherwise(pl.col(col))
                .alias(col)
            for col, val in DEFAULT_VALUES.items()
        ])

        # Flip rejection flags: rejection_reason present → rejection_status = REJECTED
        df = df.with_columns(
            rejection_status=pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE.value)
                .then(pl.lit(RejectionStatus.OVERRIDE.value))
                .otherwise(
                    pl.when(~pl.col('rejection_reason').is_null())
                        .then(pl.lit(RejectionStatus.REJECTED.value))
                        .otherwise(pl.col('rejection_status'))
                )
        )

        # Flip error flags: error_condition present → error_status = True
        df = df.with_columns(
            error_status=pl.when(pl.col('error_condition').is_null())
                .then(pl.lit(False))
                .otherwise(pl.lit(True))
        )

        # Deduplicate by hash, keeping first occurrence
        duplicates = df.filter(pl.col('hash').is_duplicated())
        if duplicates.height > 0:
            for row in duplicates.iter_rows(named=True):
                print(f"Warning: deduplicating hash {row['hash']}, Title: {row.get('original_title', 'N/A')}")
            df = df.unique(subset=['hash'], keep='first')

        # Select schema columns in order
        df = df.select(MEDIA_SCHEMA_COLUMNS)

        # Run Pandera validation
        return super().validate(df, lazy=lazy)


# -----------------------------------------------------------------------------
# Training Schema constants
# -----------------------------------------------------------------------------

TRAINING_SCHEMA_COLUMNS = [
    'imdb_id', 'tmdb_id', 'label', 'human_labeled', 'anomalous', 'media_type',
    'media_title', 'season', 'episode', 'release_year', 'budget', 'revenue',
    'runtime', 'origin_country', 'production_companies', 'production_countries',
    'production_status', 'original_language', 'spoken_languages', 'genre',
    'original_media_title', 'tagline', 'overview', 'tmdb_rating', 'tmdb_votes',
    'rt_score', 'metascore', 'imdb_rating', 'imdb_votes', 'reviewed'
]

TRAINING_POLARS_SCHEMA = {
    'imdb_id': pl.Utf8,
    'tmdb_id': pl.Int64,
    'label': pl.Categorical,
    'human_labeled': pl.Boolean,
    'anomalous': pl.Boolean,
    'media_type': pl.Categorical,
    'media_title': pl.Utf8,
    'season': pl.Int64,
    'episode': pl.Int64,
    'release_year': pl.Int64,
    'budget': pl.Int64,
    'revenue': pl.Int64,
    'runtime': pl.Int64,
    'origin_country': pl.List(pl.Utf8),
    'production_companies': pl.List(pl.Utf8),
    'production_countries': pl.List(pl.Utf8),
    'production_status': pl.Utf8,
    'original_language': pl.Utf8,
    'spoken_languages': pl.List(pl.Utf8),
    'genre': pl.List(pl.Utf8),
    'original_media_title': pl.Utf8,
    'tagline': pl.Utf8,
    'overview': pl.Utf8,
    'tmdb_rating': pl.Float64,
    'tmdb_votes': pl.Int64,
    'rt_score': pl.Int64,
    'metascore': pl.Int64,
    'imdb_rating': pl.Float64,
    'imdb_votes': pl.Int64,
    'reviewed': pl.Boolean,
}

TRAINING_DEFAULT_VALUES = {
    'human_labeled': False,
    'anomalous': False,
    'reviewed': False,
}


# -----------------------------------------------------------------------------
# Training Pandera Schema
# -----------------------------------------------------------------------------

class TrainingSchema(pa.DataFrameModel):
    """Pandera schema for training data validation."""

    # Primary key (required)
    imdb_id: str = pa.Field(
        nullable=False,
        str_length={"max_value": 10},
        str_matches=r'^tt\d{7,8}$'
    )

    # Unique identifier
    tmdb_id: Optional[int] = pa.Field(nullable=True)

    # Training classification (nullable - set in stage 06)
    label: Optional[str] = pa.Field(nullable=True)

    # Label flags
    human_labeled: bool = pa.Field(nullable=False)
    anomalous: bool = pa.Field(nullable=False)

    # Media information (required)
    media_type: str = pa.Field(nullable=False)
    media_title: str = pa.Field(nullable=False, str_length={"max_value": 255})

    # TV-specific (optional)
    season: Optional[int] = pa.Field(nullable=True)
    episode: Optional[int] = pa.Field(nullable=True)

    # Release info (required)
    release_year: int = pa.Field(nullable=False, ge=1850, le=2100)

    # Financial metadata
    budget: Optional[int] = pa.Field(nullable=True, ge=0)
    revenue: Optional[int] = pa.Field(nullable=True, ge=0)
    runtime: Optional[int] = pa.Field(nullable=True, ge=0)

    # Production info
    origin_country: Optional[List[str]] = pa.Field(nullable=True)
    production_companies: Optional[List[str]] = pa.Field(nullable=True)
    production_countries: Optional[List[str]] = pa.Field(nullable=True)
    production_status: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 25})

    # Language
    original_language: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 2})
    spoken_languages: Optional[List[str]] = pa.Field(nullable=True)

    # Descriptive
    genre: Optional[List[str]] = pa.Field(nullable=True)
    original_media_title: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 255})
    tagline: Optional[str] = pa.Field(nullable=True, str_length={"max_value": 255})
    overview: Optional[str] = pa.Field(nullable=True)

    # Ratings
    tmdb_rating: Optional[float] = pa.Field(nullable=True, ge=0, le=10)
    tmdb_votes: Optional[int] = pa.Field(nullable=True, ge=0)
    rt_score: Optional[int] = pa.Field(nullable=True, ge=0, le=100)
    metascore: Optional[int] = pa.Field(nullable=True, ge=0, le=100)
    imdb_rating: Optional[float] = pa.Field(nullable=True, ge=0, le=100)
    imdb_votes: Optional[int] = pa.Field(nullable=True, ge=0)

    # Review status
    reviewed: Optional[bool] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False

    @classmethod
    def validate(cls, df: pl.DataFrame, lazy: bool = False) -> pl.DataFrame:
        """
        Validate and normalize DataFrame:
        1. Add all missing schema columns
        2. Assign default values for boolean fields
        3. Check unique imdb_id
        4. Run Pandera validation
        5. Return columns in schema order
        """
        if df.height == 0:
            return df

        # Add all missing schema columns
        missing = [c for c in TRAINING_SCHEMA_COLUMNS if c not in df.columns]
        if missing:
            df = df.with_columns([
                pl.lit(None, dtype=TRAINING_POLARS_SCHEMA.get(c, pl.Utf8)).alias(c)
                for c in missing
            ])

        # Assign defaults where null
        df = df.with_columns([
            pl.when(pl.col(col).is_null())
                .then(pl.lit(val))
                .otherwise(pl.col(col))
                .alias(col)
            for col, val in TRAINING_DEFAULT_VALUES.items()
        ])

        # Deduplicate by imdb_id, keeping first occurrence
        duplicates = df.filter(pl.col('imdb_id').is_duplicated())
        if duplicates.height > 0:
            for row in duplicates.iter_rows(named=True):
                print(f"Warning: deduplicating imdb_id {row['imdb_id']}, Title: {row.get('media_title', 'N/A')}")
            df = df.unique(subset=['imdb_id'], keep='first')

        # Select schema columns in order
        df = df.select(TRAINING_SCHEMA_COLUMNS)

        # Run Pandera validation
        return super().validate(df, lazy=lazy)
