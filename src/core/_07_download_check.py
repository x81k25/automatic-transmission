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
# functions to operate on individual media items
# ------------------------------------------------------------------------------

# check download status of individual media item
def media_item_download_complete(hash: str):
    """
    checks to determine if media is complete for items in the downloading state
    :param hash: media item hash string value
    :return: True/False depending on download complete status
    """
    # get status from transmission
    torrent = utils.get_torrent_info(hash)

    # remove completed downloads and update status
    if torrent.progress == 100.0:
        return True
    else:
        return False

# extract filename and ensure extraction was successful
def extract_and_verify_filename(media_item: dict) -> dict:
    """
    if download is complete, extract the file name for subsequent transfer
    :param media_item: dict containing 1 row of media.df data
    :return: dict with updated file_name or error information
    """

    # get filename if download complete
    if media_item['pipeline_status'] == 'downloaded':
        torrent = utils.get_torrent_info(media_item['hash'])
        file_or_dir_name = torrent.name

        # test filename and either assign or induce error state
        if not file_or_dir_name or not isinstance(file_or_dir_name, str) or not file_or_dir_name.strip():
            media_item['error_status'] = True
            media_item['error_condition'] = "file_name must be a non-empty string"
        else:
            media_item['original_path'] = file_or_dir_name

    return media_item

# ------------------------------------------------------------------------------
# main check download function for all media items
# ------------------------------------------------------------------------------

def check_downloads(media_type: str):
    """
    check downloads for all downloading media elements, and extracts file_name
        if download is complete
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='downloading')

    # return if no media downloading
    if media is None:
        return

    # determine if downloaded, and if so change pipeline_status
    media.update(media.df.with_columns(
        pipeline_status = pl.when(pl.col('hash').map_elements(media_item_download_complete, return_dtype=pl.Boolean))
            .then(pl.lit('downloaded'))
            .otherwise(pl.col('pipeline_status'))
    ))

    # if no items complete, return
    if 'downloaded' not in media.df['pipeline_status']:
        return

    # extract file names of completed downloads
    media.update(media.df.filter(pl.col('pipeline_status') == "downloaded"))

    updated_rows = []
    for row in media.df.iter_rows(named=True):
        updated_row = extract_and_verify_filename(row)
        updated_rows.append(updated_row)

    media.update(pl.DataFrame(updated_rows))

    # report errors if present
    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['original_title']}: {row['error_condition']}")
        elif row['pipeline_status'] == "downloaded":
            logging.info(f"downloaded: {row['original_title']}")

    # update db
    utils.media_db_update(media=media)


# ------------------------------------------------------------------------------
# end of _07_download_check.py
# ------------------------------------------------------------------------------