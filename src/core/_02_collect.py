# standard library imports
import logging
import os

# third-party imports
from dotenv import load_dotenv

# local/custom imports
from src.data_models import *
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# get reel-driver env vars
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
# supporting functions
# ------------------------------------------------------------------------------

def process_new_items(new_transmission_items: dict) -> MediaDataFrame:
    """
    process new media items and return as formatted MediaDataFrame

    :param new_transmission_items: items with new hashes currently in
        transmission
    :return: MediaDataFrame with all necessary elements
    """
    new_media = MediaDataFrame(
        pl.DataFrame({
            'hash': new_transmission_items.keys(),
            'original_title': [inner_values['name'] for inner_values in new_transmission_items.values()],
            'rejection_status': RejectionStatus.OVERRIDE
        }).with_columns(
            media_type=pl.col("original_title")
                .map_elements(utils.classify_media_type, return_dtype=pl.Utf8)
        ).with_columns(
            error_condition = pl.when(pl.col('media_type') == MediaType.UNKNOWN)
                .then(pl.lit("media_type is unknown"))
                .otherwise(pl.lit(None)),
            media_title = pl.when(pl.col('media_type') != MediaType.UNKNOWN)
                .then(
                    pl.struct(['original_title', 'media_type'])
                        .map_elements(
                            lambda x: utils.extract_title(x['original_title'], x['media_type']),
                            return_dtype=pl.Utf8
                    )
                )
                .otherwise(pl.lit(None))
        )
    )

    return new_media


def update_rejected_status(rejected_media: MediaDataFrame) -> MediaDataFrame:
    """
    sets all rejections statuses to "override"

    :param rejected_media: MediaDataFrame with any previous rejection status
    :return: updated MediaDataFrame with correction rejection status
    """
    # if by error no probability was ever assigned, assign it now
    media_with_updated_status = rejected_media

    media_with_updated_status.update(
        media_with_updated_status.df.with_columns(
            rejection_status = pl.lit(RejectionStatus.OVERRIDE)
        )
    )

    return media_with_updated_status


def log_status(media: MediaDataFrame) -> None:
    """
    logs acceptance/rejection of the media filtration process

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"collected - {row['hash']}")


# ------------------------------------------------------------------------------
# collect main function
# ------------------------------------------------------------------------------

def collect_media():
    """
    collect ad hoc items added to transmission not from rss feeds and insert
    into automatic-transmission pipeline
    """
    # get torrents currently in transmission
    current_transmission_items = utils.return_current_torrents()

    # if no torrents in transmission, return
    if current_transmission_items is None:
        return

    # get current hashes
    current_hashes = list(current_transmission_items.keys())

    # process new hashes
    new_hashes = utils.compare_hashes_to_db(current_hashes)

    if len(new_hashes) > 0:
        new_transmission_items = {k: current_transmission_items[k] for k in new_hashes if k in current_transmission_items}
        new_media = process_new_items(new_transmission_items)
        utils.insert_items_to_db(media=new_media.to_schema())
        log_status(new_media)

    # process previously rejected items
    rejected_hashes = utils.return_rejected_hashes(current_hashes)

    # if no new or rejected items return
    if len(rejected_hashes) > 0:
        rejected_media = utils.get_media_by_hash(rejected_hashes)
        rejected_media.update(update_rejected_status(rejected_media).df)
        utils.media_db_update(media=rejected_media.to_schema())
        log_status(rejected_media)


# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------