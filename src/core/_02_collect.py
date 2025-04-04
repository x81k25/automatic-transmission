# standard library imports
import json
import logging
import os

# third-party imports
from dotenv import load_dotenv
import polars as pl
import requests
from sqlalchemy.orm.sync import update

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

    # create MediaDataFrame for ingestion and validation
    media = MediaDataFrame(
        pl.DataFrame({
            'hash': current_media_dict.keys(),
            'original_title': [inner_values['name'] for inner_values in current_media_dict.values()]
        }).with_columns(
            media_type=pl.col("original_title")
                .map_elements(utils.classify_media_type, return_dtype=pl.Utf8)
        ).filter(
            pl.col('media_type').is_not_null()
        )
    )

    # if no valid items, return
    if len(media.df) == 0:
        return

    # determine which items are new
    new_hashes = utils.compare_hashes_to_db(hashes=media.df['hash'].to_list())

    # determine which items were previously reject
    rejected_hashes = utils.return_rejected_hashes(hashes=media.df['hash'].to_list())

    # filter MDF for new or previously rejected hashes
    media.update(
        media.df.filter(
            pl.col('hash').is_in(new_hashes + rejected_hashes)
        )
    )

    # if no items are new or rejected hashes, return
    if len(media.df) == 0:
        return

    # set new status for new and rejcted hashes
    media.update(media.df.with_columns(
        pipeline_status = pl.lit('ingested'),
        rejections_status = pl.lit('override')
    ))

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
# code used to conduct OMDB verification of names, currently not in use as to
# not get rate limited on API
# ------------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------