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
# collect main function
# ------------------------------------------------------------------------------

def collect_media():
    """
    collect ad hoc items added to transmission not from rss feeds and insert
    into automatic-transmission pipeline
    """
    # get torrents currently in transmission
    current_media_dict = utils.return_current_torrents()

    # if no torrents in transmission, return
    if current_media_dict is None:
        return

    # get current hashes
    current_hashes = list(current_media_dict.keys())

    # determine which items are new
    new_hashes = utils.compare_hashes_to_db(current_hashes)

    # determine which items were previously reject
    rejected_hashes = utils.return_rejected_hashes(current_hashes)

    # if no new or rejected items return
    if (len(new_hashes) + len(rejected_hashes)) == 0:
        return

    new_or_rejected_hashes = new_hashes + rejected_hashes

    new_or_rejected_dict = {k: current_media_dict[k] for k in new_or_rejected_hashes if k in current_media_dict}

    # create MediaDataFrame for ingestion and validation
    media = MediaDataFrame()

    try:
        logging.debug("creating collected MediaDataFrame")
        media.append(
            pl.DataFrame({
                'hash': new_or_rejected_dict.keys(),
                'original_title': [inner_values['name'] for inner_values in new_or_rejected_dict.values()]
            }).with_columns(
                media_type=pl.col("original_title")
                    .map_elements(utils.classify_media_type, return_dtype=pl.Utf8),
                pipeline_status=pl.lit(PipelineStatus.INGESTED),
                rejection_status = pl.lit(RejectionStatus.OVERRIDE)
            ).with_columns(
                error_condition = pl.when(pl.col('media_type').is_null())
                    .then(pl.lit("media_type is unknown"))
                .otherwise(pl.lit(None))
            ).with_columns(
                media_type = pl.when(pl.col('error_condition') == "media_type is unknown")
                    .then(pl.lit(MediaType.UNKNOWN))
                .otherwise(pl.col('media_type'))
            ).with_columns(
                error_status = pl.when(~pl.col('error_condition').is_null())
                    .then(pl.lit(True))
                .otherwise(pl.lit(False))
            )
        )
    except Exception as e:
        for key in new_or_rejected_dict.keys():
            logging.error(f"error collecting media items - {key} - {e}")
        return

    # insert new items, if any
    new_mask = media.df['hash'].is_in(new_hashes)
    if new_mask.any():
        new_media = MediaDataFrame(media.df.filter(new_mask))
        utils.insert_items_to_db(media=new_media.to_schema())

    # insert previously rejected items if any
    rejected_mask = media.df['hash'].is_in(rejected_hashes)
    if rejected_mask.any():
        rejected_media = MediaDataFrame(media.df.filter(rejected_mask))
        utils.media_db_update(media=rejected_media.to_schema())

    # log collected items
    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        else:
            logging.info(f"collected - {row['hash']}")


# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------