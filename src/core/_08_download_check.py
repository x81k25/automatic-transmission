# standard library imports
import logging

# local/custom imports
from src.data_models import *
import src.utils as utils

# ------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------

# log config
utils.setup_logging()

# ------------------------------------------------------------------------------
# functions to operate on individual media items
# ------------------------------------------------------------------------------

def media_item_download_complete(hash: str) -> str | None:
    """
    checks to determine if media is complete for items in the downloading state
    :param hash: media item hash string value
    :return: True/False depending on download complete status
    """
    # attempt to get status from transmission
    try:
        torrent = utils.get_torrent_info(hash)

        # return true if download complete
        if torrent.progress == 100.0:
            return "true"
        else:
            return None

    except KeyError as e:
        logging.error(f"{hash} - not found within transmission")
        return f"{e}"
    except Exception as e:
        logging.error(f"{hash} - {e}")
        return f"{e}"


def extract_and_verify_filename(media_item: dict) -> dict:
    """
    if download is complete, extract the file name for subsequent transfer
    :param media_item: dict containing 1 row of media.df data
    :return: dict with updated file_name or error information
    """
    # if download not complete, make no change
    if media_item['download_complete'] != "true":
        return media_item

    # if download is complete
    torrent = utils.get_torrent_info(media_item['hash'])
    file_or_dir_name = torrent.name

    # test filename and either assign or induce error state
    if not file_or_dir_name or not isinstance(file_or_dir_name, str) or not file_or_dir_name.strip():
        media_item['error_condition'] = "file_name must be a non-empty string"
    else:
        media_item['original_path'] = file_or_dir_name

    return media_item


def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    updates status flags based off of conditions

    :param media: MediaDataFrame with old status flags
    :return: updated MediaDataFrame with correct status flags
    """
    media_with_updated_status = media.df.clone()

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(pl.col('rejection_status') == RejectionStatus.REJECTED)
            .then(pl.lit(PipelineStatus.REJECTED))
        .when(pl.col('download_complete') == "true")
            .then(pl.lit(PipelineStatus.DOWNLOADED))
        .otherwise(pl.lit(PipelineStatus.INGESTED))
    )

    return MediaDataFrame(media_with_updated_status)


def log_status(media: MediaDataFrame) -> None:
    """
    logs current stats of all media items

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    # log entries based on rejection status
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        if row['pipeline_status'] == PipelineStatus.INGESTED:
            logging.error(f"re-ingesting - {row['hash']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# main check download function for all media items
# ------------------------------------------------------------------------------

def check_downloads():
    """
    check downloads for all downloading media elements, and extracts file_name
        if download is complete
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='downloading')

    # return if no media downloading
    if media is None:
        return

    # determine if downloaded
    media.update(media.df.with_columns(
        download_complete=pl.col('hash').map_elements(
            media_item_download_complete, return_dtype=pl.Utf8)
    ))

    # remove items where download is not complete and there is no error
    media = MediaDataFrame(media.df.filter(~pl.col('download_complete').is_null()))

    # if no items complete or in error state return
    if media.height == 0:
        return

    # extract filename of completed downloads
    updated_rows = []
    for row in media.df.iter_rows(named=True):
        if row['pipeline_status'] == "downloaded":
            updated_row = extract_and_verify_filename(row)
            updated_rows.append(updated_row)
        else:
            updated_rows.append(row)

    media.update(pl.DataFrame(updated_rows))

    # update statuses, commit to db, and log
    media = update_status(media)
    utils.media_db_update(media=media.to_schema())
    log_status(media)


# ------------------------------------------------------------------------------
# end of _08_download_check.py
# ------------------------------------------------------------------------------