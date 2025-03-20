# standard library imports
import json
import logging
import os
import re

# third-party imports
from dotenv import load_dotenv
import polars as pl
import requests

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# initialization and setup
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# logger config
logger = logging.getLogger(__name__)

# omdb environment variables
omdb_base_url = os.getenv('OMDB_BASE_URL')
api_key = os.getenv('OMDB_API_KEY')

# ------------------------------------------------------------------------------
# metadata collection helper functions
# ------------------------------------------------------------------------------

def collect_omdb_metadata(
    media_item: dict,
    media_type: str
) -> dict:
    """
    Get metadata for a movie or TV show from the OMDb API
    :param media_item: dict containing one for of media.df
    :param media_type: type of collection, either "movie", "tv_show", or "tv_seasons"
    :return: dict of items with metadata added
    """
    #media_series = media_df.df[5]

    # Define the parameters for the OMDb API request
    if media_type == 'movie':
        params = {
            't': media_item["movie_title"],
            'y': media_item["release_year"],
            'apikey': api_key
        }
    elif media_type == 'tv_show' or media_type == 'tv_season':
        params = {
            't': media_item["tv_show_name"],
            'apikey': api_key
        }
    else:
        raise ValueError("Invalid collection type. Must be 'movie', 'tv_show', or 'tv_season'")

    # Make a request to the OMDb API
    logging.debug(f"collecting metadata for: {media_item['raw_title']} as {params['t']}")

    response = requests.get(omdb_base_url, params=params)

    status_code = response.status_code
    logging.debug(f"OMDb API call status code: {response.status_code}")

    data = json.loads(response.content)
    logging.debug(f"OMDb API response: {data}")

    # check if the response was successful, and if so move on
    if status_code == 200 and data["Response"] == "True":
        # Extract the metadata from the response
        if data:
            # items to collect for movies and tv shows
            if data.get('genre', None) != "N/A":
                media_item['genre'] = data.get('Genre', '').split(', ')
            if data.get('Language', None) != "N/A":
                media_item['language'] = data.get('Language').split(', ')
            if data.get('Metascore', None) != "N/A":
                media_item['metascore'] = data.get('Metascore')
            if data.get('imdbRating', None) != "N/A":
                media_item['imdb_rating'] = float(re.sub(r"\D", "", data.get('imdbRating')))
            if data.get('imdbVotes', None) != "N/A":
                media_item['imdb_votes'] = int(re.sub(r"\D", "", data.get('imdbVotes')))
            media_item['imdb_id'] = data.get('imdbID', None)
            # items to collect only for movies
            if media_type == 'movie':
                if "Ratings" in data:
                    # determine if Rotten tomato exists in json
                    for rating in data.get("Ratings", []):
                        if rating["Source"] == "Rotten Tomatoes":
                            media_item['rt_score'] = int(rating["Value"].rstrip('%'))
            # items to collect only for tv shows
            elif media_type == 'tv_show' or media_type == 'tv_season':
                media_item['release_year'] = int(re.search(r'\d{4}', data.get('Year', '')).group())
    # if response was not successful updates statuses appropriately
    elif status_code == 200 and data["Response"] == "False":
        media_item['status'] = 'rejected'
        media_item['rejection_reason'] = "metadata could not be collected"
        media_item['rejection_status'] = 'rejected'
    else:
        raise ValueError(f"OMDB API error for query \"{media_item["raw_title"]}\": {response_content['Error']}")

    # Save the updated tv_shows DataFrame
    return media_item

# ------------------------------------------------------------------------------
# full metadata collection pipeline
# ------------------------------------------------------------------------------

def collect_metadata(media_type: str):
    """
    Collect metadata for all movies or tv shows that have been ingested
    :param media_type: either "movie", "tv_show", or "tv_season"
    :return:
    """
    #media_type = 'movie'

    # read in existing data
    media = utils.get_media_from_db(
        media_type=media_type,
        status='parsed'
    )

    # if no media to parse, return
    if media is None:
        return

    # collect metadata for all elements
    updated_rows = []
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        # Modify your function to accept a dict instead of a Series
        updated_row = collect_omdb_metadata(row, media_type)
        updated_rows.append(updated_row)

    media.update(pl.DataFrame(updated_rows))

    # log rejected items
    for row in media.df.filter(pl.col("rejection_status") == "rejected").iter_rows(named=True):
        logging.error(f"failed to collect metadata: {row['raw_title']}")

    # update status for successfully collected items
    media.update(media.df.with_columns(
        pl.when(pl.col('status') != 'rejected')
        .then(pl.lit('metadata_collected'))
        .otherwise(pl.col('status'))
        .alias('status')
    ))

    # write metadata back to the database
    utils.media_db_update(
        media_df=media,
        media_type=media_type
    )

# ------------------------------------------------------------------------------
# end of _04_metadata_collection.py
# ------------------------------------------------------------------------------
