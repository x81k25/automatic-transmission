# standard library imports
import logging

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
    #media_type = 'movie'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='transferred'
    )

    # if no transferred items, return None
    if media is None:
        return

    # remove torrents from transmission client
    for row in media.df.iter_rows(named=True):
        utils.remove_media_item(row['hash'])
        logging.info(f"cleaned: {row['raw_title']}")

    # update status of successfully parsed items
    utils.update_db_status_by_hash(
        media_type=media_type,
        hashes=media.df['hash'].to_list(),
        new_status='complete'
    )

# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
