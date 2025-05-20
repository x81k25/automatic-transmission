# standard library imports
import logging

# third-party imports
from dotenv import load_dotenv
import polars as pl

# local/custom imports
from src.data_models import MediaDataFrame
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# logger config
logger = logging.getLogger(__name__)

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
    try:
        logging.debug("creating collected MediaDataFrame")
        media = MediaDataFrame(
            pl.DataFrame({
                'hash': new_or_rejected_dict.keys(),
                'original_title': [inner_values['name'] for inner_values in new_or_rejected_dict.values()]
            }).with_columns(
                media_type=pl.col("original_title")
                    .map_elements(utils.classify_media_type, return_dtype=pl.Utf8),
                pipeline_status=pl.lit('ingested'),
                rejection_status = pl.lit('override'),
                error_status=False,
                created_at=None
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
        utils.insert_items_to_db(media=new_media)

    # insert previously rejected items if any
    rejected_mask = media.df['hash'].is_in(rejected_hashes)
    if rejected_mask.any():
        rejected_media = MediaDataFrame(media.df.filter(rejected_mask))
        utils.media_db_update(rejected_media)

    # log collected items
    for row in media.df.iter_rows(named=True):
        logging.info(f"collected {row['media_type']}: {row['original_title']}")

# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------