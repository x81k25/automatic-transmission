# standard library imports
import json
import logging
import os
import re

# third-party imports
from dotenv import load_dotenv
import requests
import pandas as pd

# local/custom imports
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
# metadata collection helper functions
# ------------------------------------------------------------------------------

def collect_omdb_metadata(media_item, media_type):
    """
    Get metadata for a movie or TV show from the OMDb API
    :param media_item: panda series of the item to collect
    :param media_type: type of collection, either "movie" or "tv_show"
    :return:
    """

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
    response = requests.get(omdb_base_url, params=params)
    data = json.loads(response.content)

    # check if the response was successful, and if so move on
    if data["Response"] != "True":
        raise ValueError(
            f"OMDB API error for query \"{media_item["tv_show_name"]}\": {response_content['Error']}")

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

    # Save the updated tv_shows DataFrame
    return media_item

# ------------------------------------------------------------------------------
# full metadata collection pipeline
# ------------------------------------------------------------------------------

def collect_metadata(media_type):
    """
    Collect metadata for all movies or tv shows that have been ingested
    :param media_type: either "movie" or "tv_show"
    :return:
    """
    #media_type = 'movie'
    #media_type = 'tv_season'

    # read in existing data
    media = utils.get_media_from_db(
        media_type=media_type,
        status='parsed'
    )

    # select the release year of the 2nd row of the data frame
    media_collected = pd.DataFrame()

    if len(media) > 0:
        media_collected = media.copy().iloc[0:0]

        for index in media.index:
            try:
                collected_item = collect_omdb_metadata(
                    media_item=media.loc[index].copy(),
                    media_type=media_type
                )
                media_collected = pd.concat([media_collected, collected_item.to_frame().T])
                logging.info(f"metadata collected: {media_collected.loc[index, 'raw_title']}")
            except Exception as e:
                logging.error(f"failed to collect metadata: {media.loc[index, 'raw_title']}")
                logging.error(f"collect_all_metadata error: {e}")

    if len(media_collected) > 0:
        # write new elements to database
        utils.update_db_media_table(
            media_type=media_type,
            media_old=media,
            media_new=media_collected
        )

        # update status of relevant elements by hash
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=media_collected.index.tolist(),
            new_status='metadata_collected'
        )

# ------------------------------------------------------------------------------
# end of _04_metadata_collection.py
# ------------------------------------------------------------------------------
