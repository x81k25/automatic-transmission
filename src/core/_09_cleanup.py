import src.utils as utils

# ------------------------------------------------------------------------------
# function to perform cleanup for all media items
# ------------------------------------------------------------------------------

def cleanup_media(media_type):
    """
    perform final clean-up operations for torrents once all other steps have
        been verified completed succesfully
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

# ------------------------------------------------------------------------------
# end of _09_cleanup.py
# ------------------------------------------------------------------------------
