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

# load and validate pipeline env vars
load_dotenv(override=True)

target_active_items = float(os.getenv('TARGET_ACTIVE_ITEMS'))
if target_active_items < 0:
    raise ValueError(f"TARGET_ACTIVE_ITEMS value of {target_active_items} is less than 0 and no permitted")

transferred_item_cleanup_delay = float(os.getenv('TRANSFERRED_ITEM_CLEANUP_DELAY'))
if transferred_item_cleanup_delay < 0:
    raise ValueError(f"TRANSFERRED_ITEM_CLEANUP_DELAY value of {transferred_item_cleanup_delay} is less than 0 and no permitted")

hung_item_cleanup_delay = float(os.getenv('HUNG_ITEM_CLEANUP_DELAY'))
if transferred_item_cleanup_delay < 0:
    raise ValueError(f"HUNG_ITEM_CLEANUP_DELAY value of {hung_item_cleanup_delay} is less than 0 and no permitted")

# ------------------------------------------------------------------------------
# support functions
# ------------------------------------------------------------------------------

def get_delay_multiple() -> float:
    """
    returns float used to modulate all delay values

    :return: multiple by which to modulate delay values
    """
    # if TARGET_ACTIVE_ITEMS is 0, do not modulate
    if target_active_items == 0:
        return 1

    current_item_count = utils.return_current_item_count()

    # if current_item_count is 0, return 1 to avoid divisor error
    # - value will have no impact, as there is no clean_up to be done
    if current_item_count == 0:
        return 1

    # if value is 1 or greater
    delay_multiple = target_active_items/current_item_count

    return  delay_multiple


def cleanup_transferred_media(modulated_transferred_item_cleanup_delay: float):
    """
    remove items from the daemon, after set time period after which they
        have been successfully transferred

    :param modulated_transferred_item_cleanup_delay: delay time in seconds
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='transferred')

    # if no transferred items, return None
    if media is None:
        return

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
        pl.col('seconds_since_transfer') > modulated_transferred_item_cleanup_delay
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
                    f"removed transferred item - {updated_row['hash']} - {updated_row['seconds_since_transfer']}s after transfer")
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

    # update status of successfully cleaned items
    utils.media_db_update(media=media)


def cleanup_hung_items(modulated_hung_item_cleanup_delay: float):
    """
    remove items from the daemon, regardless of status, after hey have
        exceeded as set amount of time

    :param modulated_hung_item_cleanup_delay: delay time in seconds
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
        pl.col('seconds_since_transfer') > modulated_hung_item_cleanup_delay
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

    # update status of successfully cleaned items
    utils.media_db_update(media=media)


# ------------------------------------------------------------------------------
# function to perform cleanup for all media items
# ------------------------------------------------------------------------------

def cleanup_media():
    """
    perform final clean-up operations for torrents once all other steps have
        been verified completed successfully
    """
    # get delay multiple
    delay_multiple =  get_delay_multiple()

    # modulate cleanup delays and cast to integer seconds
    modulated_transferred_item_cleanup_delay = (
        int(
            (
                timedelta(days=transferred_item_cleanup_delay)
                .total_seconds()
            ) * delay_multiple
        )
    )

    modulated_hung_item_cleanup_delay = (
        int(
            (
                timedelta(days=hung_item_cleanup_delay)
                .total_seconds()
            ) * delay_multiple
        )
    )

    # remove items that have been successfully transferred
    cleanup_transferred_media(
        modulated_transferred_item_cleanup_delay=modulated_transferred_item_cleanup_delay
    )

    # remove items that have exceeded max allowed time in daemon
    cleanup_hung_items(
        modulated_hung_item_cleanup_delay = modulated_hung_item_cleanup_delay
    )


# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
