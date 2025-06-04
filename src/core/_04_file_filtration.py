# standard library imports
import logging
import os

# third-party imports
from dotenv import load_dotenv
import polars as pl
import yaml

# local/custom imports
import src.utils as utils
from src.data_models import MediaDataFrame

# -----------------------------------------------------------------------------
# read in static parameters
# -----------------------------------------------------------------------------

# load env vars
load_dotenv(override=True)

log_level = os.getenv('LOG_LEVEL', default="INFO")

if log_level == "INFO":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
elif log_level == "DEBUG":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# support functions
# ------------------------------------------------------------------------------

def filter_by_file_metadata(media_item: dict) -> dict:
    """
    filters media based off its file parameters, e.g. resolution, coded, etc.

    :param media_item:
    :return: dict containing the updated filtered data

    :debug: filter_type = 'movie'
    """
    # get filter params
    with open('./config/filter-parameters.yaml', 'r') as file:
        filters = yaml.safe_load(file)

    # pass if override status was set
    if media_item['rejection_status'] == 'override':
        return media_item

    # search separate criteria for move or tv_show
    if media_item['media_type'] == 'movie':
        sieve = filters['movie']
        # iterate over each key in the filter-parameters.yaml file
        for key in sieve:
            # if key is defined as not nullable, then item will be rejected if not populated
            if not sieve[key]["nullable"]:
                if media_item[key] is None:
                    media_item['rejection_reason'] = f'{key} is null'
                    break
            if isinstance(media_item[key], str):
                if "allowed_values" in sieve[key] and media_item[key] not in sieve[key]["allowed_values"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is not in allowed_values'
                    break
            elif isinstance(media_item[key], (int, float)):
                if "min" in sieve[key] and media_item[key] < sieve[key]["min"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is below min'
                    break
                elif "max" in sieve[key] and media_item[key] > sieve[key]["max"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is above max'
                    break
            elif isinstance(media_item[key], list):
                if "allowed_values" in sieve[key] and not any([x in sieve[key]["allowed_values"] for x in media_item[key]]):
                    media_item['rejection_reason'] = f'{key} {media_item[key]} does not include {sieve[key]["allowed_values"]}'
                    break

    # there are currently no filters set for tv_show or tv_season
    elif media_item['media_type'] == 'tv_show':
        pass
    elif media_item['media_type'] == 'tv_season':
        pass

    return media_item

# ------------------------------------------------------------------------------
# main function
# ------------------------------------------------------------------------------

def filter_files():
    """
    full pipeline for filtering all media items based off of the file metadata,
    	e.g. resolution, codec, media_type, etc.
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='parsed')

    if media is None:
        return

    # filter based off of file parameters for all elements
    updated_rows = []
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        try:
            updated_row = filter_by_file_metadata(media_item=row)
            updated_rows.append(updated_row)
        except Exception as e:
            error_message = f"file filtration error - {row['hash']} - {e}"
            logging.error(error_message)
            row['error_status'] = True
            row['error_condition'] = error_message
            updated_rows.append(row)

    media = MediaDataFrame(updated_rows)

    # commit any items with errors to db, if any
    if media.df.filter(pl.col('error_status')).height > 0:
        utils.media_db_update(
            MediaDataFrame(
                media.df.filter(pl.col('error_status'))
            )
        )

    # remove items with errors
    media.update(media.df.filter(~pl.col('error_status')))

    # update status accordingly
    media.update(
        media.df.with_columns(
            rejection_status = pl.when(
                pl.col('rejection_reason').is_null())
                    .then(pl.lit('accepted'))
                .otherwise(pl.lit('rejected'))
        ).with_columns(
            pipeline_status = pl.when(
                pl.col('rejection_status') == 'accepted')
                    .then(pl.lit('file_accepted'))
                .otherwise(pl.lit('rejected'))
        )
    )

    # log entries based on rejection status
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        if row['pipeline_status'] == 'rejected':
            logging.info(f"{row['hash']} - {row['pipeline_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['hash']} - {row['pipeline_status']} - {row['hash']}")

    # update db
    utils.media_db_update(media=media.to_schema())


# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------