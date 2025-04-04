# standard library imports
import logging
import polars as pl

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# initialization and setup
# ------------------------------------------------------------------------------

# logger config
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# utility functions
# ------------------------------------------------------------------------------

def initiate_media_item(media_item: dict) -> dict:
    """
    attempts to initiate media items, and if collects error information if
        initiation fails
    :param media_item: contains data of individual
    :return media_item: updated dict containing status and/or error data
    """
    # attempt to initiate each item
    try:
        utils.add_media_item(media_item['hash'])
        logging.info(f"downloading: {media_item['original_title']}")
        media_item['pipeline_status'] = 'downloading'
    except Exception as e:
        logging.error(f"failed to download: {media_item['original_title']}")
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
    """
    # read in existing data based
    media = utils.get_media_from_db(pipeline_status='queued')

    # if no items to initiate, then return
    if media is None:
        return

    # initiate all queued downloads
    updated_rows = []
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        updated_row = initiate_media_item(row)
        updated_rows.append(updated_row)

    media.update(pl.DataFrame(updated_rows))

    # update database with results
    utils.media_db_update(media=media)


# ------------------------------------------------------------------------------
# end of _06_initiate.py
# ------------------------------------------------------------------------------