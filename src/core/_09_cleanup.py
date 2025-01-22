import src.utils as utils

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
    # media_type = 'tv_show'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='transferred'
    )

    # remove torrents from transmission client
    for index, media_item in media.iterrows():
        utils.remove_media_item(index)

    # update status of successfully parsed items
    utils.update_db_status_by_hash(
        media_type=media_type,
        hashes=media.index.tolist(),
        new_status='complete'
    )

# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
