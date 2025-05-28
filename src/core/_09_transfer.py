# standard library imports
import logging
import os
from logging import exception

# third-party imports
from dotenv import load_dotenv
import polars as pl

# local/custom imports
from src.data_models import MediaDataFrame
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

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

# set pipeline vars
batch_size = os.getenv('BATCH_SIZE')

# set directories from .env
download_dir = os.getenv('DOWNLOAD_DIR')
tv_show_dir = os.getenv('TV_SHOW_DIR')
movie_dir = os.getenv('MOVIE_DIR')

# logger config
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# torrent clean-up functions
# ------------------------------------------------------------------------------

def transfer_item(media_item: dict) -> dict:
    """
    Transfer downloaded torrents to the appropriate directory
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
        media_item['error_status'] = True
        if media_item['error_condition'] is None:
            media_item['error_condition'] = f'failed to transfer media: {e}'
        else:
            media_item['error_condition'] = media_item['error_condition'] + \
                f'; failed to transfer media: {e}'

    return media_item


# ------------------------------------------------------------------------------
# torrent clean-up full pipelines
# ------------------------------------------------------------------------------

def transfer_media():
    """
    full pipeline for cleaning up torrents that have completed transfer
    :return:
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='downloaded')

    # if not valid media items return
    if media is None:
        return

    # batch up operations to avoid API rate limiting
    number_of_batches = (media.df.height + 49) // 50

    for batch in range(number_of_batches):
        logging.debug(f"starting transfer batch {batch+1}/{number_of_batches}")

        # set batch indices
        batch_start_index = batch * 50
        batch_end_index = min((batch + 1) * 50, media.df.height)

        # create media batch as proper MediaDataFrame to perform data validation
        media_batch = MediaDataFrame(media.df[batch_start_index:batch_end_index])

        # transfer media
        try:
            updated_rows = []
            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                updated_row = transfer_item(row)
                updated_rows.append(updated_row)

            media_batch.update(pl.DataFrame(updated_rows))

            # update pipeline_status if no error occurred
            media_batch.update(media_batch.df.with_columns(
                pipeline_status = pl.when(pl.col('error_status'))
                    .then(pl.col('pipeline_status'))
                    .otherwise(pl.lit('transferred'))
            ))

            # output error if present
            for row in media_batch.df.iter_rows(named=True):
                if row['error_status']:
                    logging.error(f"{row['hash']} - {row['error_condition']}")
                else:
                    logging.info(f"transferred - {row['hash']}")

        except Exception as e:
            # log errors to individual elements
            media_batch.update(
                media_batch.df.with_columns(
                    error_status = pl.lit(True),
                    error_condition = pl.lit(f"batch error - {e}")
                )
            )

            logging.error(f"transfer batch {batch+1}/{number_of_batches} failed - {e}")

        try:
            # attempt to write metadata back to the database; with or without errors
            utils.media_db_update(media=media_batch.to_schema())
        except Exception as e:
            logging.error(f"transfer batch {batch+1}/{number_of_batches} failed - {e}")
            logging.error(f"transfer batch error could not be stored in database")


# ------------------------------------------------------------------------------
# end of _09_transfer.py
# ------------------------------------------------------------------------------