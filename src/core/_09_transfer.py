# standard library imports
import logging
import os

# third-party imports
from dotenv import load_dotenv
import polars as pl

# local/custom imports
from src.data_models import *
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

# log config
utils.setup_logging()

# load env vars
load_dotenv(override=True)

# set directories from .env
download_dir = os.getenv('DOWNLOAD_DIR')
tv_show_dir = os.getenv('TV_SHOW_DIR')
movie_dir = os.getenv('MOVIE_DIR')

# logger config
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# media item clean-up functions
# ------------------------------------------------------------------------------

def transfer_item(media_item: dict) -> dict:
    """
    Transfer downloaded media items to the appropriate directory
    :param media_item: media dict containing one row of media.df
    :return: updated media dict that contains error info if applicable
    """
    try:
        if media_item['media_type'] == 'movie':
            utils.move_movie_local(
                dir_or_file_name=media_item['original_path'],
                download_dir=download_dir,
                movie_dir=movie_dir
            )
        elif media_item['media_type'] == 'tv_show':
            utils.move_tv_show_local(
                download_dir=download_dir,
                tv_show_dir=tv_show_dir,
                dir_or_file_name=media_item['original_path'],
                tv_show_name=media_item['media_title'],
                release_year=media_item['release_year'],
                season=media_item['season']
            )
        elif media_item['media_type'] == 'tv_season':
            utils.move_tv_season_local(
                download_dir=download_dir,
                tv_show_dir=tv_show_dir,
                dir_name=media_item['original_path'],
                tv_show_name=media_item['media_title'],
                release_year=media_item['release_year'],
                season=media_item['season']
            )
    except Exception as e:
        if media_item['error_condition'] is None:
            media_item['error_condition'] = f'failed to transfer media: {e}'
        else:
            media_item['error_condition'] = media_item['error_condition'] + \
                f'; failed to transfer media: {e}'

    return media_item


def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    updates status flags based off of conditions

    :param media: MediaDataFrame with old status flags
    :return: updated MediaDataFrame with correct status flags
    """
    media_with_updated_status = media.df.clone()

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(
            (pl.col('rejection_status').is_in([RejectionStatus.ACCEPTED.value, RejectionStatus.OVERRIDE.value])) &
            (pl.col('error_status') == False)
        ).then(pl.lit(PipelineStatus.TRANSFERRED))
        .when(pl.col('rejection_status') == RejectionStatus.REJECTED)
            .then(pl.lit(PipelineStatus.REJECTED))
        .otherwise(pl.col('pipeline_status'))
    )

    return MediaDataFrame(media_with_updated_status)


def log_status(media: MediaDataFrame) -> None:
    """
    logs current stats of all media items

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    # log entries based on rejection status
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# media item clean-up full pipelines
# ------------------------------------------------------------------------------

def transfer_media():
    """
    full pipeline for cleaning up media items that have completed transfer
    :return:
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='downloaded')

    # if not valid media items return
    if media is None:
        return

    # iterate through each transfer and update individually
    for media_item in media.df.iter_rows(named=True):

        # transfer media
        try:
            media_item = transfer_item(media_item)

            # cast to MediaDataFrame to perform validation
            media_singular = MediaDataFrame(pl.DataFrame(media_item))

            # update log and commit to db
            media_singular = update_status(media_singular)
            utils.media_db_update(media=media_singular.to_schema())
            log_status(media_singular)

        except Exception as e:

            # attempt to store and log error condition
            try:
                media_item['error_condition'] = f"{e}"

                # cast to MediaDataFrame to perform validation
                media_singular = MediaDataFrame(pl.DataFrame(media_item))

                # update log and commit to db
                media_singular = update_status(media_singular)
                utils.media_db_update(media=media_singular.to_schema())
                log_status(media_singular)

            # if attempt to store error to element fails, output error to logs
            except Exception as e:
                logging.error(f"media transfer error - {media_item['hash']} - {e}")


# ------------------------------------------------------------------------------
# end of _09_transfer.py
# ------------------------------------------------------------------------------