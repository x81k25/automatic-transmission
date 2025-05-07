# standard library imports
from datetime import datetime, UTC
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

# if not inherited set parameters here
if __name__ == "__main__" or not logger.handlers:
    # Set up standalone logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)
    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

# load environment variables in order to access CLEANUP_DELAY
load_dotenv()

# ------------------------------------------------------------------------------
# support functions
# ------------------------------------------------------------------------------

def cleanup_transferred_media():
    """
    remove items from the daemon, after set time period after which they
        have been successfully transferred
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='transferred')

    # if no transferred items, return None
    if media is None:
        return

    # cet time limit for transferred items
    cleanup_delay = int(os.getenv('TRANSFERRED_ITEM_CLEANUP_DELAY'))

    # create copy of media.df in order to add non template column
    media_exceeded = media.df.with_columns(
        seconds_since_transfer=(
            (datetime.now(UTC) - pl.col('updated_at'))
            .dt.total_seconds()
            .cast(pl.Int64)
        )
    )

    # filter for only items that have exceeded time limit
    media_exceeded = media_exceeded.filter(
        pl.col('seconds_since_transfer') > cleanup_delay
    )

    # remove items that have exceeded time limit
    updated_rows = []

    for row in media_exceeded.iter_rows(named=True):
        try:
            updated_row = row
            try:
                utils.remove_media_item(row['hash'])
                updated_row['pipeline_status'] = "complete"
                logging.info(
                    f"removed transferred item - {updated_row['hash']} - {updated_row['seconds_since_transfer']}s transfer")
                updated_rows.append(updated_row)
            except Exception as e:
                updated_row['error_status'] = True
                updated_row['error_condition'] = f"{e}"
                logging.error(
                    f"could not remove{updated_row['hash']} - {updated_row['error_condition']}")
                updated_rows.append(updated_row)
        except TypeError as e:
            # Skip rows with naive/aware datetime mismatch
            logging.warning(f"could not remove {row['hash']} - {e}")
            continue

    # if no items have successfully been removed, return
    if len(updated_rows) < 1:
        return

    # return to proper MediaDataFrame class for update
    media.update(
        pl.DataFrame(updated_rows).drop('seconds_since_transfer')
    )

    # update status of successfully parsed items
    utils.media_db_update(media=media)


def cleanup_hung_items():
    """
    remove items from the daemon, regardless of status, after hey have
        exceeded as set amount of time
    """
    # get media items currently in daemon
    current_items = utils.return_current_torrents()

    # if no current items, return
    if current_items is None:
        return

    # get hashes from current items
    hashes = list(current_items.keys())
    media = utils.get_media_by_hash(hashes)

    # if nothing to clean, return
    if media is None:
        return

    # determine items which have exceeded time limit
    cleanup_delay = int(os.getenv('HUNG_ITEM_CLEANUP_DELAY'))

    # create copy of media.df in order to add non-template column
    media_exceeded = media.df.with_columns(
        seconds_since_transfer = (
            (datetime.now(UTC) - pl.col('updated_at'))
            .dt.total_seconds()
            .cast(pl.Int64)
        )
    )

    # filter for only items that have exceeded time limit
    media_exceeded = media_exceeded.filter(
        pl.col('seconds_since_transfer') > cleanup_delay
    )

    # if not items have exceeded time limit, return
    if len(media_exceeded) < 1:
        return

    # remove items that have exceeded time limit
    updated_rows = []

    for row in media_exceeded.iter_rows(named=True):
        try:
            updated_row = row
            try:
                utils.remove_media_item(row['hash'])
                updated_row['pipeline_status'] = "rejected"
                updated_row['rejection_status'] = "rejected"
                updated_row['rejection_reason'] = "exceeded time limit"
                logging.info(
                    f"removed hung item: {updated_row['hash']} - {updated_row['seconds_since_transfer']}s after last status update")
                updated_rows.append(updated_row)
            except Exception as e:
                updated_row['error_status'] = True
                updated_row['error_condition'] = f"{e}"
                logging.error(
                    f"could not remove{updated_row['hash']} - {updated_row['error_condition']}")
                updated_rows.append(updated_row)
        except TypeError as e:
            # Skip rows with naive/aware datetime mismatch
            logging.warning(f"could not remove {row['hash']} - {e}")
            continue

    # if no items have successfully been removed, return
    if len(updated_rows) < 1:
        return

    # return to proper MediaDataFrame class for update
    media.update(
        pl.DataFrame(updated_rows).drop('seconds_since_transfer')
    )

    # update status of successfully parsed items
    utils.media_db_update(media=media)


# ------------------------------------------------------------------------------
# function to perform cleanup for all media items
# ------------------------------------------------------------------------------

def cleanup_media():
    """
    perform final clean-up operations for torrents once all other steps have
        been verified completed successfully
    """
    # remove items that have been succesfully transferred
    cleanup_transferred_media()

    # remove items that have exceeded max allowed time in daemon
    cleanup_hung_items()

# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
