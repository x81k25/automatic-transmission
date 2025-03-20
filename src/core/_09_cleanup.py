# standard library imports
from datetime import datetime, timedelta, UTC
import logging
import os

# third-party imports
from dotenv import load_dotenv
import polars as pl

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------

# logger config
logger = logging.getLogger(__name__)

# load environment variables in order to access CLEANUP_DELAY
load_dotenv()

# ------------------------------------------------------------------------------
# function to perform cleanup for all media items
# ------------------------------------------------------------------------------

def cleanup_media(
    media_type: str
):
    """
    perform final clean-up operations for torrents once all other steps have
        been verified completed successfully
    :param media_type: type of media to clean up
    """
    #media_type = 'tv_show'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='transferred'
    )

    # if no transferred items, return None
    if media is None:
        return

    cleanup_delay = int(os.getenv('CLEANUP_DELAY'))

    # filter by cleanup delay
    updated_rows = []
    for row in media.df.iter_rows(named=True):
        seconds_since_transfer = int((datetime.now(UTC) - row['updated_at']).total_seconds())
        if seconds_since_transfer >= cleanup_delay:
            updated_row = row
            try:
                utils.remove_media_item(row['hash'])
                updated_row['status'] = "complete"
                logging.info(f"cleaned: {updated_row['raw_title']}")
                updated_rows.append(updated_row)
            except Exception as e:
                updated_row['error_status'] = True
                updated_row['error_condition'] = f"{e}"
                logging.error(f"{updated_row['raw_title']}: {updated_row['error_condition']}")
                updated_rows.append(updated_row)

    # if no items have reached the cleanup_delay, return
    if len(updated_rows) == 0:
        return

    media.update(pl.DataFrame(updated_rows))

    # update status of successfully parsed items
    utils.media_db_update(
        media=media,
        media_type=media_type
    )

# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
