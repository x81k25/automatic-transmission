# standard library imports
import logging
import os
from pathlib import PurePosixPath

# third-party imports
from dotenv import load_dotenv

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

# ------------------------------------------------------------------------------
# media item clean-up functions
# ------------------------------------------------------------------------------

def generate_file_paths(media_item: dict) -> dict:
    """
    generates and adds the fields parent_path and target_path to a
        media item

    :param media_item: media_item without file paths
    :return: media_item with file paths
    """
    if media_item['media_type'] == 'movie':
        try:
            parent_path = PurePosixPath(movie_dir)
            media_item['parent_path'] = str(parent_path)
        except Exception as e:
            media_item['error_condition'] = f"error writing parent_path - {e}"

        try:
            target_path = utils.generate_movie_target_path(
                movie_title=media_item['media_title'],
                release_year=media_item['release_year'],
                resolution=media_item['resolution'],
                video_codec=media_item['video_coded']
            )
            media_item['target_path'] = str(target_path)
        except Exception as e:
            media_item['error_condition'] = f"error writing target_path - {e}"

    elif media_item['media_type'] == 'tv_season':
        try:
            parent_path = utils.generate_tv_season_parent_path(
                root_dir=tv_show_dir,
                tv_show_name=media_item['media_title'],
                release_year=media_item['release_year']
            )
            media_item['parent_path'] = str(parent_path)
        except Exception as e:
            media_item['error_condition'] = f"error writing parent_path - {e}"
        try:
            target_path = utils.generate_tv_season_target_path(
                season=media_item['season']
            )
            media_item['target_path'] = str(target_path)
        except Exception as e:
            media_item['error_condition'] = f"error writing target_path - {e}"

    elif media_item['media_type'] == 'tv_show':
        try:
            parent_path = utils.generate_tv_show_parent_path(
                root_dir=tv_show_dir,
                tv_show_name=media_item['media_title'],
                release_year=media_item['release_year'],
                season=media_item['season']
            )
            media_item['parent_path'] = str(parent_path)
        except Exception as e:
            media_item['error_condition'] = f"error writing parent_path - {e}"

        try:
            target_path = utils.generate_tv_show_target_path(
                season=media_item['season'],
                episode=media_item['episode']
            )
            media_item['target_path'] = str(target_path)
        except Exception as e:
            media_item['error_condition'] = f"error writing target_path - {e}"

    return media_item

def transfer_item(media_item: dict) -> dict:
    """
    Transfer downloaded media items to the appropriate directory
    :param media_item: media dict containing one row of media.df
    :return: updated media dict that contains error info if applicable
    """
    try:
        utils.move_dir_or_file(
            full_original_path=PurePosixPath(download_dir) / media_item['original_path'],
            full_target_path=PurePosixPath(media_item['parent_path']) / media_item['target_path']
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

    # generate files paths for items to be transferred
    updated_rows = []

    for idx, row in enumerate(media.df.iter_rows(named=True)):
        updated_row = generate_file_paths(row)
        updated_rows.append(updated_row)

    media = MediaDataFrame(updated_rows)

    # commit any errors to the database and log
    media_with_errors = MediaDataFrame(media.df.filter(pl.col('error_status')))

    if media_with_errors.height > 0:
        utils.media_db_update(media=media_with_errors.to_schema())
        log_status(media_with_errors)

        # remove from processing queue
        media.update(
            media.df.join(media_with_errors.df.select('hash'), on='hash', how='anti')
        )

    # if no media without error return
    if media.height == 0:
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