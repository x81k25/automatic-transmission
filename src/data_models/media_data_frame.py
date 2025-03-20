import polars as pl
from enum import Enum
from typing import List, Dict, Any, Optional, ClassVar, Type
from datetime import datetime

# ------------------------------------------------------------------------------
# enum classes
# ------------------------------------------------------------------------------

class MediaStatus(str, Enum):
	INGESTED = 'ingested'
	PAUSED = 'paused'
	PARSED = 'parsed'
	METADATA_COLLECTED = 'metadata_collected'
	REJECTED = 'rejected'
	QUEUED = 'queued'
	DOWNLOADING = 'downloading'
	DOWNLOADED = 'downloaded'
	TRANSFERRED = 'transferred'
	COMPLETE = 'complete'


class RejectionStatus(str, Enum):
	UNFILTERED = 'unfiltered'
	ACCEPTED = 'accepted'
	FAILED = 'failed'
	OVERRIDE = 'override'


# ------------------------------------------------------------------------------
# unified media dataframe class
# ------------------------------------------------------------------------------

class MediaDataFrame:
	"""Unified rigid polars DataFrame for all media types matching SQL schemas."""

	# Complete schema with all possible fields
	schema = {
		# Common fields
		'hash': pl.Utf8,
		'raw_title': pl.Utf8,
		'status': pl.Categorical,
		'torrent_source': pl.Utf8,
		'error_status': pl.Boolean,
		'error_condition': pl.Utf8,
		'rejection_status': pl.Categorical,
		'rejection_reason': pl.Utf8,
		'summary': pl.Utf8,
		'release_year': pl.Int64,
		'genre': pl.List(pl.Utf8),
		'language': pl.List(pl.Utf8),
		'metascore': pl.Int64,
		'imdb_rating': pl.Float64,
		'imdb_votes': pl.Int64,
		'imdb_id': pl.Utf8,
		'resolution': pl.Utf8,
		'video_codec': pl.Utf8,
		'upload_type': pl.Utf8,
		'audio_codec': pl.Utf8,
		'uploader': pl.Utf8,
		'file_name': pl.Utf8,
		'created_at': pl.Datetime,
		'updated_at': pl.Datetime,

		# Movie-specific fields
		'movie_title': pl.Utf8,
		'rt_score': pl.Int64,

		# TV-specific fields
		'tv_show_name': pl.Utf8,
		'season': pl.Int64,
		'episode': pl.Int64
	}

	# Common required columns
	required_columns = ['hash', 'raw_title']

	def __init__(self, data: Optional[Any] = None):
		"""
		Initialize with data that can be converted to a polars DataFrame.

		Args:
			data: Data to convert to DataFrame
		"""
		if data is None:
			# Create empty DataFrame with proper schema
			self._df = pl.DataFrame(schema=self.schema)
		elif isinstance(data, pl.DataFrame):
			self._validate_and_prepare(data)
		else:
			# Try to create from other data types (dict, list, etc.)
			try:
				self._df = pl.DataFrame(data)
				self._validate_and_prepare(self._df)
			except Exception as e:
				raise ValueError(
					f"Could not create MediaDataFrame from data: {e}")

	def _validate_and_prepare(self, df: pl.DataFrame) -> None:
		"""
		Validate that DataFrame conforms to the required schema and prepare it.

		Args:
			df: DataFrame to validate
		"""
		# Check required columns
		missing = [col for col in self.required_columns if
				   col not in df.columns]
		if missing:
			raise ValueError(f"Missing required columns: {missing}")

		# Set the underlying DataFrame
		self._df = df

	def update(self, df: pl.DataFrame):
		"""Update internal DataFrame directly."""
		self._validate_and_prepare(df)
		self._df = df

	@property
	def df(self) -> pl.DataFrame:
		"""Access the underlying polars DataFrame."""
		return self._df


# ------------------------------------------------------------------------------
# end of data_models.py
# ------------------------------------------------------------------------------