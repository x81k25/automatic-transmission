# standard library imports
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

# load env vars
load_dotenv(override=True)

log_level = os.getenv('LOG_LEVEL', default="INFO")

if log_level == "INFO":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
elif log_level == "DEBUG":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# functions to operate on individual media items
# ------------------------------------------------------------------------------

def media_item_download_complete(hash: str) -> str:
    """
    checks to determine if media is complete for items in the downloading state
    :param hash: media item hash string value
    :return: True/False depending on download complete status
    """
    # attempt to get status from transmission
    try:
        torrent = utils.get_torrent_info(hash)

        # return true if download compelte
        if torrent.progress == 100.0:
            return "true"
        else:
            return "false"

    except KeyError as e:
        logging.error(f"{hash} - not found within transmission")
        return "error"
    except Exception as e:
        logging.error(f"{hash} - {e}")
        logging.info(f"re-ingesting: {hash}")
        return "error"


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

    # change pipeline_status based off of download status and error_status
    #     if error condition is present re-ingest item
    #     due to db trigger, setting pipeline_status = 'ingested' will reset
    #     error_condition, error_status, rejection_status, rejection_reason
    media.update(media.df.with_columns(
        pipeline_status=pl.when(pl.col("download_complete") == "true")
            .then(pl.lit("downloaded"))
            .when(pl.col("download_complete") == "error")
            .then(pl.lit('ingested'))
            .otherwise(pl.col('pipeline_status'))
    ).drop('download_complete'))

    # if no items complete or with error, return
    if media.df.filter(pl.col('pipeline_status').is_in(['downloaded', 'ingested'])).height == 0:
        return

    # update media to contain only items complete or in error state
    media.update(
        media.df.filter(
            pl.col('pipeline_status').is_in(['downloaded', 'ingested']))
    )

    # extract filename of completed downloads
    updated_rows = []
    for row in media.df.iter_rows(named=True):
        if row['pipeline_status'] == "downloaded":
            updated_row = extract_and_verify_filename(row)
            updated_rows.append(updated_row)
        else:
            updated_rows.append(row)

    # Update the dataframe with the processed rows
    media.update(pl.DataFrame(updated_rows))

    # report errors if present
    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['pipeline_status'] == "downloaded":
            logging.info(f"downloaded - {row['hash']}")
        elif row['pipeline_status'] == "ingested":
            logging.info(f"re-ingesting - {row['hash']}")

    # update db
    utils.media_db_update(media=media.to_schema())

# ------------------------------------------------------------------------------
# end of _08_download_check.py
# ------------------------------------------------------------------------------