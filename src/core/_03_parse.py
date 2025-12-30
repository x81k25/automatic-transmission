# standard library imports
import logging
from pathlib import Path

# third-party imports
import yaml

# local/custom imports
from src.data_models import MediaSchema, PipelineStatus
import src.utils as utils
import polars as pl

# ------------------------------------------------------------------------------
# title parse helper functions
# ------------------------------------------------------------------------------

def parse_media_items(media: pl.DataFrame) -> pl.DataFrame:
    """
    Parse the title of media items to extract relevant information
    :param media: DataFrame contain all elements to be parsed
    :returns: DataFrame with parsed elements
    """
    # For normal execution
    try:
        config_path = Path(__file__).parent.parent.parent / 'config' / 'string-special-conditions.yaml'
    # For IDE/interactive execution
    except NameError:
        config_path = './config/string-special-conditions.yaml'

    with open(config_path, 'r') as file:
        special_conditions = yaml.safe_load(file)

    # Create a copy of the input DataFrame
    parsed_media = media.clone()

    # Add missing columns with null values (these columns will be populated during parsing)
    optional_cols = ['release_year', 'season', 'episode']
    for col in optional_cols:
        if col not in parsed_media.columns:
            parsed_media = parsed_media.with_columns(pl.lit(None).cast(pl.Int64).alias(col))

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

    # extract media_title
    parsed_media = parsed_media.with_columns(
        media_title=pl.struct(["cleaned_title", "media_type"]).map_elements(
                lambda x: utils.extract_title(x["cleaned_title"], x["media_type"]),
                return_dtype=pl.Utf8
        )
    )

    # drop the cleaned title
    parsed_media.drop_in_place('cleaned_title')

    return parsed_media


def validate_parsed_media(media: pl.DataFrame) -> pl.DataFrame:
    """
    validates media data based on the media type and update error status columns.
    :param media : DataFrame containing parsed elements
    :returns verified_media: DataFrame with verification check elements
        contained within
    """
    verified_media = media.clone()

    # Add error_condition column if missing
    if 'error_condition' not in verified_media.columns:
        verified_media = verified_media.with_columns(pl.lit(None).cast(pl.Utf8).alias('error_condition'))

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


def update_status(media: pl.DataFrame) -> pl.DataFrame:
    """
    updates status flags based off of conditions

    :param media: DataFrame with old status flags
    :return: updated DataFrame with correct status flags
    """
    # Add error_condition column if missing
    updated_media = media.clone()
    if 'error_condition' not in updated_media.columns:
        updated_media = updated_media.with_columns(pl.lit(None).cast(pl.Utf8).alias('error_condition'))

    # update status of successfully parsed items (check error_condition directly
    # since error_status is flipped later by MediaSchema.validate)
    return updated_media.with_columns(
        pipeline_status=pl.when(pl.col('error_condition').is_null())
            .then(pl.lit(PipelineStatus.PARSED.value))
            .otherwise(pl.col('pipeline_status'))
    )


def log_status(media: pl.DataFrame) -> None:
    """
    logs current stats of all media items

    :param media: DataFrame contain process values to be printed
    :return: None
    """
    for row in media.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# full title parse pipeline
# ------------------------------------------------------------------------------

def parse_media():
    """
    full ingest pipeline for either movies or tv shows
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status=PipelineStatus.INGESTED)

    # if no new data, return
    if media is None:
        return

    # parse and validate media items
    media = parse_media_items(media)
    media = validate_parsed_media(media)

    # write to db and update status
    media = update_status(media)
    media = MediaSchema.validate(media)
    utils.media_db_update(media=media)
    log_status(media)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    parse_media()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _03_parse.py
# ------------------------------------------------------------------------------