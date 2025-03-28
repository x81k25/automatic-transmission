# standard library imports
import logging

# third-party imports
from dotenv import load_dotenv
import polars as pl
import yaml

# local/custom imports
from src.data_models import MediaDataFrame
import src.utils as utils

# ------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------

# load environment variables
load_dotenv()

# logger config
logger = logging.getLogger(__name__)

# read in string special conditions
with open('./config/string-special-conditions.yaml', 'r') as file:
    special_conditions = yaml.safe_load(file)

# ------------------------------------------------------------------------------
# title parse helper functions
# ------------------------------------------------------------------------------

def parse_media_items(
    media: MediaDataFrame,
    media_type: str
) -> pl.DataFrame:
    """
    Parse the title of media items to extract relevant information
    :param media: MediaDataFrame contain all elements to be parsed
    :param media_type: either "movie", "tv_show", or "tv_season"
    :returns: DataFrame with parsed elements
    """
    # Create a copy of the input DataFrame
    parsed_media_df = media.df.clone()

    # Apply pre-processing replacements
    parsed_media_df = parsed_media_df.with_columns(
        cleaned_title = pl.col("raw_title")
    )

    for old_str, new_str in special_conditions['pre_processing_replacements']:
        parsed_media_df = parsed_media_df.with_columns(
            cleaned_title = pl.col("cleaned_title").str.replace(old_str, new_str)
        )

    # Extract common patterns using vectorized operations
    parsed_media_df = parsed_media_df.with_columns(
        resolution=pl.col("cleaned_title").map_elements(utils.extract_resolution, return_dtype=pl.Utf8),
        video_codec=pl.col("cleaned_title").map_elements(utils.extract_video_codec, return_dtype=pl.Utf8),
        audio_codec=pl.col("cleaned_title").map_elements(utils.extract_audio_codec, return_dtype=pl.Utf8),
        upload_type=pl.col("cleaned_title").map_elements(utils.extract_upload_type, return_dtype=pl.Utf8),
        uploader=pl.col("cleaned_title").map_elements(utils.extract_uploader, return_dtype=pl.Utf8)
    )

    # Process based on media type
    if media_type == "movie":
        # Extract movie-specific fields
        parsed_media_df = parsed_media_df.with_columns(
            release_year=pl.col("cleaned_title").map_elements(utils.extract_year, return_dtype=pl.Int32)
        )
    elif media_type == "tv_show":
        # Extract season and episode numbers
        parsed_media_df = parsed_media_df.with_columns(
            season=pl.col("cleaned_title").map_elements(
                utils.extract_season_from_episode, return_dtype=pl.Int32),
            episode=pl.col("cleaned_title").map_elements(
                utils.extract_episode_from_episode, return_dtype=pl.Int32)
        )
    elif media_type == "tv_season":
        # Extract season number
        parsed_media_df = parsed_media_df.with_columns(
            season=pl.col("cleaned_title").map_elements(
                utils.extract_season_from_season, return_dtype=pl.Int32)
        )

    # extract media_title; will always be last step
    for old_str, new_str in special_conditions['post_processing_replacements']:
        parsed_media_df = parsed_media_df.with_columns(
            cleaned_title = pl.col("cleaned_title").str.replace(old_str, new_str)
        )

    if media_type == 'movie':
        # Extract movie-specific fields
        parsed_media_df = parsed_media_df.with_columns(
            movie_title=pl.col("cleaned_title").map_elements(
                lambda x: utils.extract_title(x, media_type),
                return_dtype=pl.Utf8)
        )
    elif media_type == "tv_show":
        # Extract TV show name
        parsed_media_df = parsed_media_df.with_columns(
            tv_show_name=pl.col("cleaned_title").map_elements(
                lambda x: utils.extract_title(x, media_type),
                return_dtype=pl.Utf8)
        )
    elif media_type == "tv_season":
        # Extract TV show name
        parsed_media_df = parsed_media_df.with_columns(
            tv_show_name=pl.col("cleaned_title").map_elements(
                lambda x: utils.extract_title(x, media_type))
        )

    # drop the cleaned title
    parsed_media_df.drop_in_place('cleaned_title')

    return parsed_media_df


def validate_parsed_media(
    parsed_media: MediaDataFrame,
    media_type: str
) -> pl.DataFrame:
    """
    validates media data based on the media type and update error status columns.
    :param parsed_media : MediaDataFrame containing parsed elements
    :param media_type: type of media: "movie", "tv_show", or "tv_season"
    :returns verified_media: MediaDataFrame with verification check elements
        contained within
    """
    verified_media = parsed_media.df.clone()

    # Define mandatory fields for each media type
    mandatory_fields = {
        'movie': ['movie_title', 'release_year'],
        'tv_show': ['tv_show_name', 'season', 'episode'],
        'tv_season': ['tv_show_name', 'season']
    }

    # Validate based on media type
    if media_type not in mandatory_fields:
        raise ValueError(f"Invalid media_type: {media_type}")

    required = mandatory_fields[media_type]

    # For each required field, check if it exists and is not empty
    for field in required:
        # Check for null values
        null_mask = pl.col(field).is_null()

        # For string columns, also check for empty strings
        if verified_media.schema[field] == pl.Utf8:
            null_mask = null_mask | (pl.col(field) == "")

        # Update error status and condition for rows with missing values
        verified_media = verified_media.with_columns(
            error_status=pl.when(null_mask)
                .then(pl.lit(True))
                .otherwise(pl.col("error_status")),
            error_condition=pl.when(null_mask)
                .then(
                    pl.when(pl.col('error_condition').is_null())
                        .then(pl.lit(f"empty or null {field}"))
                        .otherwise(pl.concat_str(
                            pl.col('error_condition'),
                            pl.lit(f"; empty or null {field}")
                        ))
                    )
                .otherwise(pl.col('error_condition'))
        )

    return verified_media


# ------------------------------------------------------------------------------
# full title parse pipeline
# ------------------------------------------------------------------------------

def parse_media(media_type: str):
    """
    full ingest pipeline for either movies or tv shows
    :param media_type: either "movie" or "tv_show"
    """
    #media_type='movie'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='ingested'
    )

    # if no new data, return
    if media is None:
        return

    # iterate through all new movies, parse data from the title and add to new dataframe
    media.update(parse_media_items(media, media_type))

    # validate all essential fields are present
    media.update(validate_parsed_media(media, media_type))

    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['raw_title']}: {row['error_condition']}")
        else:
            logging.info(f"parsed: {row['raw_title']}")

    # update status of successfully parsed items
    media.update(media.df.with_columns(
        status=pl.when(~pl.col('error_status'))
        .then(pl.lit('parsed'))
        .otherwise(pl.col('status'))
    ))

    # write parsed data back to the database
    utils.media_db_update(
        media=media,
        media_type=media_type
    )

# ------------------------------------------------------------------------------
# end of _03_parse.py
# ------------------------------------------------------------------------------