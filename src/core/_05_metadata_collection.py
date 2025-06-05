# standard library imports
import json
import logging
import os
import re

# third-party imports
from dotenv import load_dotenv
import requests

# local/custom imports
from src.data_models import *
import src.utils as utils

# -----------------------------------------------------------------------------
# initialization and setup
# -----------------------------------------------------------------------------

# log config
utils.setup_logging()

# load env vars
load_dotenv(override=True)

# pipeline env vars
#stale_metadata_threshold = int(os.getenv('STALE_METADATA_THRESHOLD'))
batch_size = int(os.getenv('BATCH_SIZE'))

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
# API collection and processing functions
# ------------------------------------------------------------------------------

def media_search(media_item: dict) -> dict:
    """
    uses TMDB to search for a media item; the TMDB search API is less strict
        than its actual media retrieval API; this step is intended to be one
        additional check on the string parsing of the original_title field

    :param media_item: dict containing one for of media.df
    :return: dict of items with metadata added

    :debug: media_item = media.df.row(88, named=True)
    """
    # make API call for media search
    response = {}

    if media_item['media_type'] == 'movie':
        params = {
            'query': media_item["media_title"],
            'api_key': movie_search_api_key
        }

        # add year if available
        if media_item['release_year'] is not None:
            params['year'] = media_item["release_year"]

        logging.debug(f"searching for: {media_item['hash']} as '{params['query']}' - '{params['year']}'")

        # make a request to the API
        response = requests.get(movie_search_api_base_url, params=params)

    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        params = {
            'query': media_item["media_title"],
            'api_key': tv_search_api_key
        }

        # add year if available
        if media_item['release_year'] is not None:
            params['year'] = media_item['release_year']

        logging.debug(f"searching for: {media_item['hash']} as '{params['query']}'")

        # Make a request to the media API
        response = requests.get(tv_search_api_base_url, params=params)

    # verify successful API response and update status accordingly
    if response.status_code != 200:
        logging.error(f"media search API returned status code: {response.status_code}")
        media_item['error_condition'] = f"media search API returned status code: {response.status_code}"
        return media_item

    # verify that contents exist within the data object and update status accordingly
    if len(json.loads(response.content)['results']) == 0:
        logging.debug(f'no metadata could be found for: {media_item['hash']} as "{media_item['media_title']}"')
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

    :debug: media_item = row
    """
    #media_item = media.df.row(0, named=True)
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

    # collect id fields
    if 'imdb_id' in data:
        media_item['imdb_id'] = data['imdb_id']

    # collect time information
    # if release_year cannot be set, reject item
    year_pattern = r'(19|20)\d{2}'
    if media_item['media_type'] == 'movie':
        if 'release_date' in data and data['release_date'] != '':
            release_year = re.search(year_pattern, data.get('release_date'))[0]
            media_item['release_year'] = int(release_year)
        elif bool(re.search(year_pattern, str(media_item['release_year']))):
            pass
        else:
            media_item['rejection_reason'] = f"release_year could not be extracted from tmdb details API"
            media_item['rejection_status'] = 'rejected'
            return media_item
    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        release_year = re.search(year_pattern, data.get('first_air_date'))[0]
        media_item['release_year'] = int(release_year)

    # collect quantiative fields
    if 'budget' in data:
        media_item['budget'] = data['budget']
    # collect revenue
    if 'revenue' in data:
        media_item['revenue'] = data['revenue']
    # collect runtime
    if 'runtime' in data:
        media_item['runtime'] = data['runtime']

    # collect country and production information
    # collect origin country
    if 'origin_country' in data:
        origin_country = []
        for country in data['origin_country']:
            origin_country.append(country)
        media_item['origin_country'] = origin_country

    # collect production companies
    if 'production_companies' in data:
        production_companies = []
        for company in data['production_companies']:
            production_companies.append(company['name'])
        media_item['production_companies'] = production_companies

    # collect production countries
    if 'production_countries' in data:
        production_countries = []
        for country in data['production_countries']:
            production_countries.append(country['iso_3166_1'])

    # collect production status
    if 'status' in data:
        media_item['production_status'] = data['status']

    # collect language information
    # collect original language
    if 'original_language' in data:
        media_item['original_language'] = data['original_language']

    # collect spoken languages
    if 'spoken_languages' in data:
        spoken_languages = []
        for language in data.get('spoken_languages'):
            spoken_languages.append(language['iso_639_1'])
        media_item['spoken_languages'] = spoken_languages

    # collect other stings fields
    # collect genres
    if 'genres' in data:
        genres = []
        for genre in data.get('genres'):
            genres.append(genre['name'])
        media_item['genre'] = genres

    # collect original title
    if 'original_title' in data:
        media_item['original_media_title'] = data['original_title']

    # collect long strings
    # collect media overview
    if 'overview' in data:
        media_item['overview'] = data['overview']

    # collect tagline
    if 'tagline' in data:
        media_item['tagline'] = data['tagline']

    # collect TMDB ratgins
    if 'vote_average' in data:
        media_item['tmdb_rating'] = data['vote_average']
    if 'vote_count' in data:
        media_item['tmdb_votes'] = data['vote_count']

    return media_item


def collect_ratings(media_item: dict) -> dict:
    """
    get ratings specific details for each media item, e.g. rt_score, metascore

    :param media_item: dict containing one for of media.df
    :return: dict of items with metadata added
    """
    response = {}

    # Define the parameters for the OMDb API request
    if media_item['media_type'] == 'movie':
        # if available query by imdb_id
        if media_item['imdb_id'] is not None:
            params = {
                'i': media_item["imdb_id"],
                'apikey': movie_ratings_api_key
            }
        else:
            params = {
                't': media_item["media_title"],
                'apikey': movie_ratings_api_key
            }
            if media_item['release_year'] is not None:
                params['y'] = media_item["release_year"],

        logging.debug(f"collecting ratings for: {media_item['hash']}")
        response = requests.get(movie_ratings_api_base_url, params=params)

    elif media_item['media_type'] in ['tv_show', 'tv_season']:
        # if available query by imdb_id
        if media_item['imdb_id'] is not None:
            params = {
                'i': media_item["imdb_id"],
                'apikey': movie_ratings_api_key
            }
        else:
            params = {
                't': media_item["media_title"],
                'apikey': movie_ratings_api_key
            }
            if media_item['release_year'] is not None:
                params['y'] = media_item["release_year"],

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
# individual unit processing functions
# ------------------------------------------------------------------------------

def process_media_with_existing_metadata(
    media: MediaDataFrame,
    existing_metadata: pl.DataFrame
) -> MediaDataFrame:
    """
    combines the existing metadata with on imdb_id with the current
        MediaDataFrame

    :param media: entire media set, or media batch set
    :param existing_metadata: existing metadata indexed IMDB
    :return: MediaDataFrame with metadata attached and ready for upload
    """
    media_with_metadata = media.df.clone()
    metadata_to_join = existing_metadata.clone()

    # if metadata is already collected and not stale, use previously collected data
    media_with_metadata = (
        media_with_metadata
            .filter(pl.col('tmdb_id').is_in(metadata_to_join['tmdb_id'].to_list()))
            .update(
                metadata_to_join,
                on='tmdb_id'
            )
    )

    return MediaDataFrame(media_with_metadata)


# ------------------------------------------------------------------------------
# update and log status functions
# ------------------------------------------------------------------------------

def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    updates status flags based off of conditions

    :param media: MediaDataFrame with old status flags
    :return: updated MediaDataFrame with correct status flags
    """
    media_with_updated_status = media.df.clone()

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(
            (pl.col('rejection_status').is_in([RejectionStatus.ACCEPTED.value, RejectionStatus.OVERRIDE.value])) &
            (pl.col('error_status') == False)
        ).then(pl.lit(PipelineStatus.METADATA_COLLECTED))
        .when(pl.col('rejection_status') == RejectionStatus.REJECTED)
            .then(pl.lit(PipelineStatus.REJECTED))
        .otherwise(pl.col('pipeline_status'))
    )

    return MediaDataFrame(media_with_updated_status)


def log_status(media: MediaDataFrame) -> None:
    """
    logs current stats of all media items

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    # log entries based on rejection status
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# full metadata collection pipeline
# ------------------------------------------------------------------------------

def collect_metadata():
    """
    Collect metadata for all movies or tv shows that have been ingested

    :debug: batch=0
    """
    # read in existing data
    media = utils.get_media_from_db(pipeline_status='file_accepted')

    # if no media to parse, return
    if media is None:
        return

    # batch up operations to avoid API rate limiting
    number_of_batches = (media.df.height + (batch_size-1)) // batch_size

    for batch in range(number_of_batches):
        logging.debug(f"starting metadata collection batch {batch+1}/{number_of_batches}")

        # set batch indices
        batch_start_index = batch * batch_size
        batch_end_index = min((batch + 1) * batch_size, media.df.height)

        # create media batch as proper MediaDataFrame to perform data validation
        media_batch = MediaDataFrame(media.df[batch_start_index:batch_end_index])

        try:
            # search for media, and if not available reject
            updated_rows = []

            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                updated_row = media_search(row)
                updated_rows.append(updated_row)

            media_batch.update(pl.DataFrame(updated_rows))

            # determine if metadata is already collected
            existing_metadata = utils.get_media_metadata(list(set(media_batch.df['tmdb_id'])))

            # if metadata already collected, apply to df and commit to db
            if existing_metadata is not None:
                media_batch_with_metadata = process_media_with_existing_metadata(
                    media_batch,
                    existing_metadata
                )

                media_batch_with_metadata = update_status(media_batch_with_metadata)
                utils.media_db_update(media_batch_with_metadata.to_schema())
                log_status(media_batch_with_metadata)

                # filter for items which still need metadata collection
                media_batch.update(
                    media_batch.df.filter(~pl.col('hash').is_in(media_batch_with_metadata.df['hash'].to_list()))
                )

            # if all items already collected, move on to next batch
            if media_batch.df.height == 0:
                continue

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

            # update status, commit to db, and log
            media_batch = update_status(media_batch)
            utils.media_db_update(media=media_batch.to_schema())
            log_status(media_batch)

            logging.debug(f"completed metadata collection batch {batch+1}/{number_of_batches}")

        except Exception as e:
            logging.error(f"metadata collection batch {batch+1}/{number_of_batches} failed - {e}")


# ------------------------------------------------------------------------------
# end of _04_metadata_collection.py
# ------------------------------------------------------------------------------
