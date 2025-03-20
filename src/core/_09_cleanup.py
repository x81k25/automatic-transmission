# standard library imports
import logging

# third-party imports
import polars as pl

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------

# logger config
logger = logging.getLogger(__name__)

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

    # remove torrents from transmission client
    updated_rows = []
    for row in media.df.iter_rows(named=True):
        try:
            utils.remove_media_item(row['hash'])
            row['status'] = "transferred"
            logging.info(f"cleaned: {row['raw_title']}")
            updated_rows.append(row)
        except Exception as e:
            row['error_status'] = True
            row['error_condition'] = f"{e}"
            logging.error(f"{row['raw_title']}: {row['error_condition']}")
            updated_rows.append(row)

    media.update(pl.DataFrame(updated_rows))

    # update status of successfully parsed items
    utils.update_db_status_by_hash(
        media_type=media_type,
        hashes=media.df['hash'].to_list(),
        new_status='complete'
    )

# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
