# standard library imports
import json
import logging
import os
import re
import time
from logging import exception

# third-party imports
from dotenv import load_dotenv
import polars as pl
import requests

# local/custom imports
from src.data_models import MediaDataFrame
import src.utils as utils

# ------------------------------------------------------------------------------
# initialization and setup
# ------------------------------------------------------------------------------

# logger config
logger = logging.getLogger(__name__)

# if not inherited set parameters here
if __name__ == "__main__" or not logger.handlers:
    # Set up standalone logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)
    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

# Load environment variables from .env file
load_dotenv(override=True)

# pipeline env vars
batch_size = os.getenv('BATCH_SIZE')

# load api env vars
movie_search_api_base_url = os.getenv('MOVIE_SEARCH_API_BASE_URL')
movie_details_api_base_url = os.getenv('MOVIE_DETAILS_API_BASE_URL')
movie_ratings_api_base_url = os.getenv('MOVIE_RATINGS_API_BASE_URL')
movie_search_api_key = os.getenv('MOVIE_SEARCH_API_KEY')
movie_details_api_key = os.getenv('MOVIE_DETAILS_API_KEY')
movie_ratings_api_key = os.getenv('MOVIE_RATINGS_API_KEY')

tv_search_api_base_url = os.getenv('TV_SEARCH_API_BASE_URL')
tv_details_api_base_url = os.getenv('TV_DETAILS_API_BASE_URL')
tv_ratings_api_base_url = os.getenv('TV_RATINGS_API_BASE_URL')
tv_search_api_key = os.getenv('TV_SEARCH_API_KEY')
tv_details_api_key = os.getenv('TV_DETAILS_API_KEY')
tv_ratings_api_key = os.getenv('TV_RATINGS_API_KEY')

# ------------------------------------------------------------------------------
# metadata collection helper functions
# ------------------------------------------------------------------------------

def media_search(media_item: dict) -> dict:
    """
    uses TMDB to search for a media item; the TMDB search API is less strict
        than its actual media retrieval API; this step is intended to be one
        additional check on the string parsing of the original_title field

    :param media_item: dict containing one for of media.df
    :return: dict of items with metadata added
    """
    #media_item = media.df.row(0, named=True)
    # make API call for media search
    response = {}

    if media_item['media_type'] == 'movie':
        params = {
            'query': media_item["media_title"],
            'year': media_item["release_year"],
            'api_key': movie_search_api_key
        }
        logging.debug(f"searching for: {media_item['hash']} as '{params['query']}' - '{params['year']}'")

        response = requests.get(movie_search_api_base_url, params=params)

    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        params = {
            'query': media_item["media_title"],
            'api_key': tv_search_api_key
        }
        logging.debug(f"searching for: {media_item['hash']} as '{params['query']}'")

        # Make a request to the media API
        response = requests.get(tv_search_api_base_url, params=params)

    # verify successful API response and update status accordingly
    if response.status_code != 200:
        logging.error(f"media search API returned status code: {response.status_code}")
        media_item['error_status'] = True
        media_item['error_condition'] = f"media search API returned status code: {response.status_code}"
        return media_item

    # verify that contents exist within the data object and update status accordingly
    if len(json.loads(response.content)['results']) == 0:
        logging.debug(f'no metadata could be found for: {media_item['hash']} as "{media_item['media_title']}"')
        media_item['pipeline_status'] = 'rejected'
        media_item['error_status'] = False
        media_item['error_condition'] = None
        media_item['rejection_status'] = 'rejected'
        media_item['rejection_reason'] = f"media search failed"
        return media_item

    # if no issue load results
    data = json.loads(response.content)['results'][0]

    # re-assign title to media_item based off of query search
    query_title = None
    if media_item['media_type'] == 'movie':
        query_title = data.get('title')
    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        query_title = data.get('name')

    if query_title != media_item['media_title']:
        logging.debug(
            f"for item {media_item['hash']} changing title from {media_item['media_title']} -> {query_title}"
        )
        media_item['media_title'] = query_title

    # store tmdb_id
    media_item['tmdb_id'] = int(data.get('id'))

    # Save the updated tv_shows DataFrame
    return media_item


def collect_details(media_item: dict) -> dict:
    """
    uses TMDB to get the details of a media item; this is a different API
        then the one above, which accepts only the tmdb_id collected above

    :param media_item: dict containing one for of media.df
    :return: dict of items with metadata added
    """
    #media_item = media.df.row(15, named=True)
    response = {}

    # prepare and send response
    if media_item['media_type'] == 'movie':
        params = {'api_key': movie_details_api_key}
        url = f"{movie_details_api_base_url}/{media_item['tmdb_id']}"

        logging.debug(f"collecting metadata details for: {media_item['hash']}")
        response = requests.get(url, params=params)
    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        params = {'api_key': tv_details_api_key}
        url = f"{tv_details_api_base_url}/{media_item['tmdb_id']}"

        logging.debug(f"collecting metadata details for: {media_item['hash']}")
        response = requests.get(url, params=params)

    # verify successful API response and update status accordingly
    if response.status_code != 200:
        logging.error(f"media details API returned status code: {response.status_code}")
        media_item['error_status'] = True
        media_item['error_condition'] = f"media details API status code: {response.status_code}"
        return media_item

    # if no issue load results
    data = json.loads(response.content)

    # metadata items common to all media types
    genres = []
    for genre in data.get('genres'):
        genres.append(genre['name'])

    media_item['genre'] = genres

    languages = []
    languages.append(data.get('original_language'))
    for language in data.get('spoken_languages'):
        languages.append(language['iso_639_1'])
    languages=list(set(languages))

    media_item['language'] = languages

    # media type specific fields to collect
    if media_item['media_type'] == 'movie':
        media_item['imdb_id'] = data.get('imdb_id')
    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        year_pattern = r'(19|20)\d{2}'
        release_year = re.search(year_pattern, data.get('first_air_date'))[0]
        media_item['release_year'] = int(release_year)

    # potential fields to include at a later data
    #created_by
    #networks
    #origin_country
    #languages
    #overview
    #popularity
    #production_companies
    #production_countries

    return media_item


def collect_ratings(media_item: dict) -> dict:
    """
    get ratings specific details for each media item, e.g. rt_score, metascore

    :param media_item: dict containing one for of media.df
    :return: dict of items with metadata added
    """
    # media_item = media.df.row(5, named=True)

    response = {}

    # Define the parameters for the OMDb API request
    if media_item['media_type'] == 'movie':
        params = {
            't': media_item["media_title"],
            'y': media_item["release_year"],
            'apikey': movie_ratings_api_key
        }

        logging.debug(f"collecting ratings for: {media_item['hash']}")
        response = requests.get(movie_ratings_api_base_url, params=params)

    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        params = {
            't': media_item["media_title"],
            'y': media_item["release_year"],
            'apikey': tv_ratings_api_key
        }

        logging.debug(f"collecting ratings for: {media_item['hash']}")
        response = requests.get(tv_ratings_api_base_url, params=params)

    status_code = response.status_code

    if response.status_code != 200:
        logging.error(f"media ratings API returned status code: {response.status_code}")
        media_item['error_condition'] = f"media ratings API status code: {response.status_code}"
        return media_item

    data = json.loads(response.content)

    # check if the response was successful, and if so move on
    if status_code == 200 and data["Response"] == "True":
        # Extract the metadata from the response
        if data:
            # items to collect for movies and tv shows
            if data.get('Metascore', None) != "N/A":
                media_item['metascore'] = data.get('Metascore')
            if data.get('imdbRating', None) != "N/A":
                media_item['imdb_rating'] = float(re.sub(r"\D", "", data.get('imdbRating')))
            if data.get('imdbVotes', None) != "N/A":
                media_item['imdb_votes'] = int(re.sub(r"\D", "", data.get('imdbVotes')))
            media_item['imdb_id'] = data.get('imdbID', None)
            # items to collect only for movies
            if media_item['media_type'] == 'movie':
                if "Ratings" in data:
                    # determine if Rotten tomato exists in json
                    for rating in data.get("Ratings", []):
                        if rating["Source"] == "Rotten Tomatoes":
                            media_item['rt_score'] = int(rating["Value"].rstrip('%'))
            # items to collect only for tv shows

    # return the updated media_item
    return media_item


# ------------------------------------------------------------------------------
# full metadata collection pipeline
# ------------------------------------------------------------------------------

def collect_metadata():
    """
    Collect metadata for all movies or tv shows that have been ingested

    :debug: batch = 0
    """
    # read in existing data
    media = utils.get_media_from_db(pipeline_status='parsed')

    # if no media to parse, return
    if media is None:
        return

    # batch up operations to avoid API rate limiting
    number_of_batches = (media.df.height + 49) // 50

    for batch in range(number_of_batches):
        logging.debug(f"starting metadata collection batch {batch+1}/{number_of_batches}")

        # set batch indices
        batch_start_index = batch * 50
        batch_end_index = min((batch + 1) * 50, media.df.height)

        # create media batch as proper MediaDataFrame to perform data validation
        media_batch = MediaDataFrame(media.df[batch_start_index:batch_end_index])

        try:
            # search for media, and if not available reject
            updated_rows = []

            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                updated_row = media_search(row)
                updated_rows.append(updated_row)

            media_batch.update(pl.DataFrame(updated_rows))

            # get additional media details
            updated_rows = []

            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                if not row['error_status'] and row['rejection_status'] != 'rejected':
                    updated_row = collect_details(row)
                    updated_rows.append(updated_row)
                else:
                    updated_rows.append(row)

            media_batch.update(pl.DataFrame(updated_rows))

            # get media rating metadata
            updated_rows = []

            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                if not row['error_status'] and row['rejection_status'] != 'rejected':
                    updated_row = collect_ratings(row)
                    updated_rows.append(updated_row)
                else:
                    updated_rows.append(row)

            media_batch.update(pl.DataFrame(updated_rows))

            # if metadata successfully collected update status
            media_batch.update(
                media_batch.df.with_columns(
                    pipeline_status = pl.when(
                        (~pl.col('error_status')) & (pl.col('rejection_status') != 'rejected')
                    ).then(pl.lit('metadata_collected'))
                    .otherwise(pl.col('pipeline_status'))
                )
            )

            # log successfully collected items
            for idx, row in enumerate((media_batch.df.filter(pl.col('pipeline_status') == 'metadata_collected')).iter_rows(named=True)):
                logging.info(f"metadata collected - {row['hash']}")

            logging.debug(f"completed metadata collection batch {batch+1}/{number_of_batches}")

        except Exception as e:
            # log errors to individual elements
            media_batch.update(
                media_batch.df.with_columns(
                    error_status = pl.lit(True),
                    error_condition = pl.lit(f"batch error - {e}")
                )
            )

            logging.error(f"metadata collection batch {batch+1}/{number_of_batches} failed - {e}")

        try:
            # attempt to write metadata back to the database; with or without errors
            utils.media_db_update(media=media_batch)
        except Exception as e:
            logging.error(f"metadata collection batch {batch+1}/{number_of_batches} failed - {e}")
            logging.error(f"metadata collection batch error could not be stored in database")


# ------------------------------------------------------------------------------
# end of _04_metadata_collection.py
# ------------------------------------------------------------------------------
