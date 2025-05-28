# standard library imports
import logging
import os
import polars as pl

# third party imports
from dotenv import load_dotenv

# local/custom imports
from src.data_models import *
import src.utils as utils

# ------------------------------------------------------------------------------
# initialization and setup
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

# pipeline env vars
batch_size = os.getenv('BATCH_SIZE')

# ------------------------------------------------------------------------------
# utility functions
# ------------------------------------------------------------------------------

def initiate_media_item(media_item: dict) -> dict:
    """
    attempts to initiate media items, and if collects error information if
        initiation fails
    :param media_item: contains data of individual
    :return media_item: updated dict containing status and/or error data

    :debug:
    """
    # attempt to initiate each item
    try:
        utils.add_media_item(media_item['hash'])
        logging.info(f"downloading - {media_item['hash']}")
        media_item['pipeline_status'] = PipelineStatus.DOWNLOADING
    except Exception as e:
        logging.error(f"failed to download - {media_item['hash']}")
        logging.error(f"initiate_item error: {e}")

        media_item['error_status'] = True
        media_item['error_condition'] = f"initiate_item error: {e}"

    return media_item


# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def initiate_media_download():
    """
    attempts to initiate all media elements currently in the queued state

    :debug: batch=0
    """
    # read in existing data based
    media = utils.get_media_from_db(pipeline_status=PipelineStatus.MEDIA_ACCEPTED)

    # if no items to initiate, then return
    if media is None:
        return

     # batch up operations to avoid API rate limiting
    number_of_batches = (media.df.height + 49) // 50

    for batch in range(number_of_batches):
        logging.debug(f"starting initiation batch {batch+1}/{number_of_batches}")

        # set batch indices
        batch_start_index = batch * 50
        batch_end_index = min((batch + 1) * 50, media.df.height)

        # create media batch as proper MediaDataFrame to perform data validation
        media_batch = MediaDataFrame(media.df[batch_start_index:batch_end_index])

        try:
            # initiate all queued downloads
            updated_rows = []
            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                updated_row = initiate_media_item(row)
                updated_rows.append(updated_row)

            media_batch.update(pl.DataFrame(updated_rows))

            logging.debug(f"completed initiation batch {batch+1}/{number_of_batches}")
        except Exception as e:
            # log errors to individual elements
            media_batch.update(
                media_batch.df.with_columns(
                    error_status = pl.lit(True),
                    error_condition = pl.lit(f"batch error - {e}")
                )
            )

            logging.error(f"initiation batch {batch+1}/{number_of_batches} failed - {e}")

        try:
            # attempt to write metadata back to the database; with or without errors
            utils.media_db_update(media=media_batch.to_schema())
        except Exception as e:
            logging.error(f"initiation batch {batch+1}/{number_of_batches} failed - {e}")
            logging.error(f"initiation batch error could not be stored in database")


# ------------------------------------------------------------------------------
# end of _07_initiate.py
# ------------------------------------------------------------------------------