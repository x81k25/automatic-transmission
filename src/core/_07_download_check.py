import pandas as pd
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# functions to operate on individual media items
# ------------------------------------------------------------------------------

# check download status of individual media item
def media_item_download_complete(hash):
    # Instantiate transmission client
    torrent = utils.get_torrent_info(hash)

    # remove completed downloads and update status
    if torrent.progress == 100.0:
        # get file name
        return True
    else:
        return False

# extract filename and ensure extraction was successful
def extract_and_verify_filename(media_item):
    # get filename
    torrent = utils.get_torrent_info(media_item.name)
    file_name = torrent.name

    # test filename and if it is not the desired state raise error
    if not file_name or not isinstance(file_name, str) or not file_name.strip():
        raise ValueError("file_name must be a non-empty string")

    # assign filename if no error raised
    media_item.file_name = file_name

    return media_item

# ------------------------------------------------------------------------------
# main check download function for all media items
# ------------------------------------------------------------------------------
def check_downloads(media_type):
    """
    Full pipeline for cleaning up torrents

    :param media_type: type of cleanup, either 'movie' or 'tv_show'
    """
    # debug statements
    #media_type = 'tv_show'
    #media_type = 'movie'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='downloading'
    )

    # if no media are downloading conclude function
    if len(media) == 0:
        return

    # convert individual df hashes to list
    media_hashes = media.index.tolist()
    media_hashes_download_complete = []

    # determine if download is complete for each media item
    for hash in media_hashes:
        try:
            if media_item_download_complete(hash):
                media_hashes_download_complete.append(hash)
        except Exception as e:
            utils.log(f"failed to check downloads status of: {media.loc[hash, 'raw_title']}")
            utils.log(f"download_check error: {e}")

    # if no downloads are complete, conclude function
    if len(media_hashes_download_complete) == 0:
        return

    # instantiate download complete dataframe
    #media_download_complete = pd.DataFrame()
    media_download_complete = media.copy().iloc[0:0]

    # attempt extact of the filenames for completed downloads
    for hash in media_hashes_download_complete:
        try:
            download_complete_item = extract_and_verify_filename(media_item=media.loc[hash].copy())
            media_download_complete = pd.concat([media_download_complete, download_complete_item.to_frame().T])
            utils.log(f"download complete: {media.loc[hash, 'raw_title']}")
        except Exception as e:
            utils.log(f"failed to extract filename from: {media.loc[hash, 'raw_title']}")
            utils.log(f"download_check error: {e}")

   # update database with filename
    utils.update_db_media_table(
        media_type=media_type,
        media_old=media,
        media_new=media_download_complete
    )

    # update status of relevant elements by hash
    utils.update_db_status_by_hash(
        media_type=media_type,
        hashes=media_download_complete.index.tolist(),
        new_status='downloaded'
    )

# ------------------------------------------------------------------------------
# end of _07_download_check.py
# ------------------------------------------------------------------------------