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
load_dotenv(override=True)

# logger config
logger = logging.getLogger(__name__)

# read in string special conditions
with open('./config/string-special-conditions.yaml', 'r') as file:
    special_conditions = yaml.safe_load(file)

# ------------------------------------------------------------------------------
# title parse helper functions
# ------------------------------------------------------------------------------

def parse_media_items(media: MediaDataFrame) -> pl.DataFrame:
    """
    Parse the title of media items to extract relevant information
    :param media: MediaDataFrame contain all elements to be parsed
    :returns: DataFrame with parsed elements
    """
    # Create a copy of the input DataFrame
    parsed_media = media.df.clone()

    # Apply pre-processing replacements
    parsed_media = parsed_media.with_columns(
        cleaned_title = pl.col("original_title")
    )

    for old_str, new_str in special_conditions['pre_processing_replacements']:
        parsed_media = parsed_media.with_columns(
            cleaned_title = pl.col("cleaned_title").str.replace(old_str, new_str)
        )

    # Extract common patterns using vectorized operations
    parsed_media = parsed_media.with_columns(
        resolution=pl.col("cleaned_title").map_elements(utils.extract_resolution, return_dtype=pl.Utf8),
        video_codec=pl.col("cleaned_title").map_elements(utils.extract_video_codec, return_dtype=pl.Utf8),
        audio_codec=pl.col("cleaned_title").map_elements(utils.extract_audio_codec, return_dtype=pl.Utf8),
        upload_type=pl.col("cleaned_title").map_elements(utils.extract_upload_type, return_dtype=pl.Utf8),
        uploader=pl.col("cleaned_title").map_elements(utils.extract_uploader, return_dtype=pl.Utf8)
    )

    # process based on media type
    movie_mask = parsed_media['media_type'] == "movie"
    if movie_mask.any():
        parsed_media = parsed_media.with_columns(
            release_year = pl.when(movie_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_year,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('release_year'))
        )

    tv_show_mask = parsed_media['media_type'] == "tv_show"
    if tv_show_mask.any():
        parsed_media = parsed_media.with_columns(
            season = pl.when(tv_show_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_season_from_episode,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('season')),
            episode = pl.when(tv_show_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_episode_from_episode,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('episode'))
        )

    tv_season_mask = parsed_media['media_type'] == "tv_season"
    if tv_season_mask.any():
        parsed_media = parsed_media.with_columns(
            season=pl.when(tv_season_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_season_from_season,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('season'))
        )

    # extract media_title; will always be last step
    for old_str, new_str in special_conditions['post_processing_replacements']:
        parsed_media = parsed_media.with_columns(
            cleaned_title = pl.col("cleaned_title").str.replace(old_str, new_str)
        )

    parsed_media = parsed_media.with_columns(
        media_title=pl.when(pl.col('media_title').is_null()).then(
            pl.struct(["cleaned_title", "media_type"]).map_elements(
                lambda x: utils.extract_title(x["cleaned_title"], x["media_type"]),
                return_dtype=pl.Utf8
            )
        ).otherwise(pl.col('media_title'))
    )

    # drop the cleaned title
    parsed_media.drop_in_place('cleaned_title')

    return parsed_media


def validate_parsed_media(media: MediaDataFrame) -> pl.DataFrame:
    """
    validates media data based on the media type and update error status columns.
    :param media : MediaDataFrame containing parsed elements
    :returns verified_media: MediaDataFrame with verification check elements
        contained within
    """
    verified_media = media.df.clone()

    # Define mandatory fields for each media type
    mandatory_fields = {
        'all_media': ['media_title'],
        'movie': ['release_year'],
        'tv_show': ['season', 'episode'],
        'tv_season': ['season']
    }

    # verify mandatory fields for all media types
    for field in mandatory_fields['all_media']:
        verified_media = verified_media.with_columns(
            error_status = pl.when(pl.col(field).is_null())
                .then(pl.lit(True))
                .otherwise(pl.col('error_status')),
            error_condition = pl.when(pl.col(field).is_null())
                .then(
                    pl.when(pl.col('error_condition').is_null())
                    .then(pl.lit(f"{field} is null"))
                    .otherwise(pl.concat_str(
                        pl.col('error_condition'),
                        pl.lit(f"; {field} is null")
                    ))
                )
                .otherwise(pl.col('error_condition'))
        )

    # verify mandatory fields for specific media types
    movie_mask = verified_media['media_type'] == "movie"
    if movie_mask.any():
        for field in mandatory_fields['movie']:
            null_mask = pl.col(field).is_null()
            verified_media = verified_media.with_columns(
                error_status=pl.when(movie_mask).then(
                    pl.when(null_mask)
                        .then(pl.lit(True))
                        .otherwise(pl.col("error_status")),
                ).otherwise('error_status'),
                error_condition=pl.when(movie_mask).then(
                    pl.when(null_mask)
                        .then(
                            pl.when(pl.col('error_condition').is_null())
                            .then(pl.lit(f"{field} is null"))
                            .otherwise(pl.concat_str(
                                pl.col('error_condition'),
                                pl.lit(f"; {field} is null")
                            ))
                        )
                        .otherwise(pl.col('error_condition'))
                ).otherwise('error_condition')
            )

    tv_show_mask = verified_media['media_type'] == "tv_show"
    if tv_show_mask.any():
        for field in mandatory_fields['tv_show']:
            null_mask = pl.col(field).is_null()
            verified_media = verified_media.with_columns(
                error_status=pl.when(tv_show_mask).then(
                    pl.when(null_mask)
                        .then(pl.lit(True))
                        .otherwise(pl.col("error_status"))
                ).otherwise('error_status'),
                error_condition=pl.when(tv_show_mask).then(
                    pl.when(null_mask)
                        .then(
                            pl.when(pl.col('error_condition').is_null())
                            .then(pl.lit(f"{field} is null"))
                            .otherwise(pl.concat_str(
                                pl.col('error_condition'),
                                pl.lit(f"; {field} is null")
                            ))
                        )
                        .otherwise(pl.col('error_condition'))
                ).otherwise('error_condition')
            )

    tv_season_mask = verified_media['media_type'] == "tv_season"
    if tv_season_mask.any():
        for field in mandatory_fields['tv_season']:
            null_mask = pl.col(field).is_null()
            verified_media = verified_media.with_columns(
                error_status=pl.when(tv_season_mask).then(
                    pl.when(null_mask)
                        .then(pl.lit(True))
                        .otherwise(pl.col("error_status"))
                ).otherwise('error_status'),
                error_condition=pl.when(tv_season_mask).then(
                    pl.when(null_mask)
                        .then(
                            pl.when(pl.col('error_condition').is_null())
                            .then(pl.lit(f"{field} is null"))
                            .otherwise(pl.concat_str(
                                pl.col('error_condition'),
                                pl.lit(f"; {field} is null")
                            ))
                        )
                        .otherwise(pl.col('error_condition'))
                ).otherwise('error_condition')
            )

    return verified_media


# ------------------------------------------------------------------------------
# full title parse pipeline
# ------------------------------------------------------------------------------

def parse_media():
    """
    full ingest pipeline for either movies or tv shows
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='ingested')

    # if no new data, return
    if media is None:
        return

    # iterate through all new movies, parse data from the title and add to new dataframe
    media.update(parse_media_items(media=media))

    # validate all essential fields are present
    media.update(validate_parsed_media(media))

    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['original_title']}: {row['error_condition']}")
        else:
            logging.info(f"parsed: {row['original_title']}")

    # update status of successfully parsed items
    media.update(media.df.with_columns(
        pipeline_status=pl.when(~pl.col('error_status'))
            .then(pl.lit('parsed'))
            .otherwise(pl.col('pipeline_status'))
    ))

    # write parsed data back to the database
    utils.media_db_update(media=media)

# ------------------------------------------------------------------------------
# end of _03_parse.py
# ------------------------------------------------------------------------------