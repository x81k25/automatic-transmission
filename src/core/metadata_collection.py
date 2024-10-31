from dotenv import load_dotenv
import pickle
import os
import re
import requests
from src.utils import logger, safe

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# omdb environment variables
omdb_base_url = os.getenv('omdb_base_url')
api_key = os.getenv('omdb_api_key')

# ------------------------------------------------------------------------------
# metadata collection helper functions
# ------------------------------------------------------------------------------

def collect_omdb_metadata(item_to_collect, collection_type):
    """
    Get metadata for a movie or TV show from the OMDb API
    :param item_to_collect: panda series of the item to collect
    :param collection_type: type of collection, either "movie" or "tv_show"
    :return:
    """
    # Define the parameters for the OMDb API request
    if collection_type == 'movie':
        params = {
            't': item_to_collect["movie_title"],
            'y': item_to_collect["release_year"],
            'apikey': api_key  # Your OMDb API key
        }
    elif collection_type == 'tv_show':
        params = {
            't': item_to_collect["tv_show_name"],
            'apikey': api_key  #
        }
    else:
        raise ValueError("Invalid collection type. Must be 'movie' or 'tv_show'")

    # Make a request to the OMDb API
    response = requests.get(omdb_base_url, params=params)

    # Check if the response was successful
    if response.status_code == 200:
        data = response.json()
    else:
        data = None
        logger(f"failed to retrieve metadata from OMDB: {item_to_collect['raw_title']}")

    # Extract the metadata from the response
    if data and data['Response'] == 'True':
        # items to collect for movies and tv shows
        if data.get('genre', None) != "N/A":
            item_to_collect['genre'] = data.get('Genre', '').split(', ')
        if data.get('Language', None) != "N/A":
            item_to_collect['language'] = data.get('Language').split(', ')
        if data.get('Metascore', None) != "N/A":
            item_to_collect['metascore'] = data.get('Metascore')
        if data.get('imdbRating', None) != "N/A":
            item_to_collect['imdb_rating'] = float(re.sub(r"\D", "", data.get('imdbRating')))
        if data.get('imdbVotes', None) != "N/A":
            item_to_collect['imdb_votes'] = int(re.sub(r"\D", "", data.get('imdbVotes')))
        item_to_collect['imdb_id'] = data.get('imdbID', None)
        # items to collect only for movies
        if collection_type == 'movie':
            if "Ratings" in data:
                # determine if Rotten tomato exists in json
                for rating in data.get("Ratings", []):
                    if rating["Source"] == "Rotten Tomatoes":
                        item_to_collect['rt_score'] = int(rating["Value"].rstrip('%'))
        # items to collect only for tv shows
        elif collection_type == 'tv_show':
            item_to_collect['release_year'] = int(re.sub(r'\D', '', data.get('Year', None)))

    # Save the updated tv_shows DataFrame
    return item_to_collect

# ------------------------------------------------------------------------------
# full metadata collection pipeline
# ------------------------------------------------------------------------------

def collect_all_metadata(metadata_type):
    """
    Collect metadata for all movies or tv shows that have been ingested
    :param metadata_type: either "movie" or "tv_show"
    :return:
    """
    # read in existing data based on ingest_type
    #metadata_type = 'movie'
    if metadata_type == 'movie':
        master_df_dir = './data/movies.pkl'
    elif metadata_type == 'tv_show':
        master_df_dir = './data/tv_shows.pkl'
    else:
        raise ValueError("invalid ingest type. Must be 'movie' or 'tv_show'")

    with open(master_df_dir, 'rb') as file:
        master_df = pickle.load(file)

    # select only the rows of the data frame that have a status of ingested
    hashes_to_collect = master_df[master_df['status'] == "ingested"].index

    # select the release year of the 2nd row of the data frame
    if len(hashes_to_collect) > 0:
        for index in hashes_to_collect:
            try:
                collected_item = collect_omdb_metadata(
                    item_to_collect=master_df.loc[index].copy(),
                    collection_type=metadata_type
                )
                master_df = safe.assign_row(master_df, index, collected_item)
                master_df = safe.update_status(master_df, index, 'queued')
                logger(f"queued: {master_df.loc[index, 'raw_title']}")
            except Exception as e:
                logger(f"failed to queue: {master_df.loc[index, 'raw_title']}")
                logger(f"collect_all_metadata error: {e}")

    # write tv shows back to csv
    with open(master_df_dir, 'wb') as file:
        pickle.dump(master_df, file)

# ------------------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
