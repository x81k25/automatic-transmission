# standard library imports
import json
import logging
import os

# third-party imports
from dotenv import load_dotenv
import polars as pl
import requests

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

# omdb environment variables
omdb_base_url = os.getenv('OMDB_BASE_URL')
api_key = os.getenv('OMDB_API_KEY')

# ------------------------------------------------------------------------------
# collector helper functions
# ------------------------------------------------------------------------------

def verify_omdb_retrievable(cleaned_title: str):
    """
    determine if item is OMDB retrievable to confirm if the title parsing
    was conducted successfully; no metadata will actually be retrieved at this
    point; metadata retrieval will be conducted in _04_metadata_collection.py
    :param cleaned_title: cleaned string of name of media item
    :raises: ValueError if API query fails
    """
    #cleaned_title = "the matrix"
    #cleaned_title = "oiawjefoijawefjawe"

    # Define the parameters for the OMDb API request
    params = {
        't': cleaned_title,
        'apikey': api_key  # Your OMDb API key
    }

    # Make a request to the OMDb API
    response = requests.get(omdb_base_url, params=params)
    response_content = json.loads(response.content)

    # check if the response was successful, and if so move on
    if response_content["Response"] == "True":
        return
    else:
        raise ValueError(f"OMDB API error for query \"{cleaned_title}\": {response_content['Error']}")


# ------------------------------------------------------------------------------
# collect main function
# ------------------------------------------------------------------------------

def collect_media(media_type: str):
    """
    collect ad hoc items added to transmission not from rss feeds and insert
    into automatic-transmission pipeline
    :param media_type: type of media to collect, either 'movie' or 'tv_show'
    """
    #media_type = 'movie'

    # get torrents currently in transmission
    # if no torrents in transmission end function
    current_media_dict = utils.return_current_torrents()

    if current_media_dict is None:
        return

    # convert response to pl.DataFrame
    rows = []
    for hash_id, inner_dict in current_media_dict.items():
        if isinstance(inner_dict, dict):
            # Create a row with hash and all attributes
            row = {'hash': hash_id}
            for k, v in inner_dict.items():
                row[k] = v
            rows.append(row)

    media_df = pl.DataFrame(rows)
    media_df = media_df.rename({'name': 'raw_title'})

    # determine media type and keep only the desired type
    media_df = media_df.with_columns(
        pl.col("raw_title")
            .map_elements(utils.classify_media_type, return_dtype=pl.Utf8)
            .alias("media_type")
    ).filter(
        pl.col('media_type').is_not_null()
    ).filter(
        pl.col('media_type') == media_type
    )

    # if no items match classification end function
    if len(media_df) == 0:
        return

    # extract clean item name from raw_title
    # media_df = media_df.with_columns(
    #     cleaned_title = pl.col('raw_title')
    #         .map_elements(lambda x: utils.extract_title(x, media_type), return_dtype=pl.Utf8)
    # )

    # determine if item is omdb retrievable, if not raise error and remove from items
    # to_remove = []

    #commenting out, current backlog is eating up OMDB queue
    # for hash_id, item_data in current_media.items():
    #     try:
    #         verify_omdb_retrievable(cleaned_title=item_data['cleaned_title'])
    #     except Exception as e:
    #         to_remove.append(hash_id)
    #         logging.error(f"failed to retrieve OMDB metadata: {current_media[hash_id]['name']}")
    #         logging.error(f"collect_media error: {e}")

    # for hash_id in to_remove:
    #     del current_media[hash_id]
    #
    # # if no items are OMDB retrievable end function
    # if len(current_media) == 0:
    #     return

    # determine which items are new
    new_hashes = utils.compare_hashes_to_db(
        media_type=media_type,
        hashes=media_df['hash'].to_list()
    )

    # determine which items were previously reject
    rejected_hashes = utils.return_rejected_hashes(
        media_type=media_type,
        hashes=media_df['hash'].to_list()
    )

    # convert new items to MediaDataFrame for db ingestion
    new_media = MediaDataFrame(
        media_df.select(['hash', 'raw_title', 'torrent_source'])
            .filter(pl.col('hash').is_in(new_hashes))
    )

    # add new items to db and set statuses
    if len(new_media.df) > 0:

        # insert new items to db
        utils.insert_items_to_db(
            media_type=media_type,
            media=new_media
        )

        # update status
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=new_media.df['hash'].to_list(),
            new_status='ingested'
        )

        # update rejection status to override
        utils.update_rejection_status_by_hash(
            media_type=media_type,
            hashes=new_media.df['hash'].to_list(),
            new_status='override'
        )

        # log collected items
        for row in new_media.df.iter_rows(named=True):
            logging.info(f"collected {media_type}: {row['raw_title']}")

    # update statuses of items that have been previously rejected
    rejected_media_df = media_df.filter(pl.col('hash').is_in(rejected_hashes))

    if len(rejected_hashes) > 0:
        # update status
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=rejected_hashes,
            new_status='ingested'
        )

        # update rejection status
        utils.update_rejection_status_by_hash(
            media_type=media_type,
            hashes=rejected_hashes,
            new_status='override'
        )

        # log items that have been previously rejected, with the rejection overridden
        for row in rejected_media_df.iter_rows(named=True):
            logging.info(f"collected {media_type}: {row['raw_title']}")

# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------