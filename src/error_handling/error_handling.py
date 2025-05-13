# standard imports
import logging
import os
import time
import sys

# third-party imports
import psycopg2
from psycopg2.extensions import connection
import yaml

# custom and internal imports
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables and params
# ------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("paramiko").setLevel(logging.INFO)

# ------------------------------------------------------------------------------
# supporting functions used by multiple error_handling functions
# ------------------------------------------------------------------------------

def create_conn(env: str) -> psycopg2.extensions.connection:
    """
    created connection object with given items from the connection yaml based
        off of the env provided

    :param env: env name as a string to connect to
    :return: psycopg2 connection based off of the env provided
    """
    # Read the YAML configuration file
    try:
        with open('environments.yaml', 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError("environments.yaml file not found")

    # Get the database configuration for the specified environment
    if env not in config['pgsql']:
        raise ValueError(f"Environment '{env}' not found in configuration")

    db_config = config['pgsql'][env]

    # Create the connection string
    conn_string = f"host={db_config['endpoint']} " \
                  f"port={db_config['port']} " \
                  f"dbname={db_config['database_name']} " \
                  f"user={db_config['username']} " \
                  f"password={db_config['password']}"

    # Connect to the database
    conn = psycopg2.connect(conn_string)

    # Set the schema as the default for lookups
    with conn.cursor() as cursor:
        cursor.execute(f"SET search_path TO {db_config['schema']}")
        conn.commit()

    return conn


def get_all_hashes(env: str) -> list:
    """
    returns all hashes from the select environment media table

    :param env: either "prod" "stg" or "dev" used for mapping to appropriate
        ports and credentials
    :returns hashes: list of media item hashes
    """
    # get database con
    conn = create_conn(env)

    # construct udpate statement
    statement = f"""select hash from atp.media order by hash"""

    with conn.cursor() as cursor:
        cursor.execute(statement)
        results = cursor.fetchall()
        hashes = [row[0] for row in results]

    conn.close()

    return hashes


# ------------------------------------------------------------------------------
# error_handling functions
# ------------------------------------------------------------------------------

def recycle_downloading_items(
    media_type: str,
    port: int = 9093,
    schema: str = "dev"
):
    """
    - removes all items currently downloading from the database
    - upon rerun of pipeline, all elements this function removed should be
      collected and properly re-inserted into pipeline

    example execution:

    clean_downloading_items(
        media_type = "tv_show"
        ,port = 9091
        ,schema = "prod"
    )

    :param media_type: either "movies", "tv_shows", or "tv_seasons"
    :param port: the given port number for the cleaning;
      - 9091 for prod
      - 9092 for stg
      - 9093 for dev (default)
    :param schema: matching schema name for port number
    """
    #port = 9091

    # get all current downloading torrents
    current_items = utils.return_current_torrents(port=port)

    if current_items is None:
        logging.info("no items to clean")
        return

    current_hashes = list(current_items.keys())

    logging.debug(f"current hashes: {current_hashes}")

    # delete them from the database
    utils.delete_items_from_db(
        hashes=current_hashes,
        media_type=media_type,
        schema=schema
    )


def mark_items_as_complete(
    hashes: list,
    env: str
) -> int:
    """
    - marks items as pipeline_status = "complete"
    - removes any error flags or conditions
    - set rejection status to override

    :param hashes: list of hashes as strings to be marked as complete
    :param env: either "prod" "stg" or "dev" used for mapping to appropriate
        ports and credentials
    :returns: int of the number of rows successfully updated
    """
    # get database con
    conn = create_conn(env)

    # convert list to string
    hash_string = ",".join([f"'{h}'" for h in hashes])

    # construct udpate statement
    statement = f"""
    update atp.media 
    set pipeline_status = 'complete',
        error_status = False,
        error_condition = Null,
        rejection_status = 'override',
        rejection_reason = Null
    where hash in (
        {hash_string}
    )
    """

    with conn.cursor() as cursor:
        cursor.execute(statement)
        rows_updated = cursor.rowcount
        conn.commit()

    conn.close()

    return rows_updated


def reingest_items(
    hashes: list,
    env: str
) -> int:
    """
    takes elements at any stage in the ingestion pipeline and puts them back
        into the initial ingested state

    :param hashes: list of hashes as strings to be marked as complete
    :param env: either "prod" "stg" or "dev" used for mapping to appropriate
        ports and credentials
    :returns: int of the number of rows successfully updated
    """
    # get database con
    conn = create_conn(env)

    # convert list to string
    hash_string = ",".join([f"'{h}'" for h in hashes])

    # construct udpate statement
    statement = f"""
    update atp.media
    set pipeline_status = 'ingested', 
        error_status = False,
        error_condition = Null,
        rejection_status = 'unfiltered',
        rejection_reason = Null
    where hash in (
        {hash_string}
    )
    """

    with conn.cursor() as cursor:
        cursor.execute(statement)
        rows_updated = cursor.rowcount
        conn.commit()

    conn.close()

    return rows_updated


def reject_hung_downloads(
    hashes: list,
    env: str
) -> int:
    """
    takes downloads that are just taking too long and updates status
        appropriately; in many cases these will be items that are redundant
        to other items that have already been successful; also remove the
        item from the daemon

    :param hashes: list of hashes as strings to be marked as complete
    :param env: either "prod" "stg" or "dev" used for mapping to appropriate
        ports and credentials
    :returns: int of the number of rows successfully updated
    """
    # get database con
    conn = create_conn(env)

    # convert list to string
    hash_string = ",".join([f"'{h}'" for h in hashes])

    # construct udpate statement
    statement = f"""
    update atp.media
    set pipeline_status = 'rejected', 
        error_status = False,
        error_condition = Null,
        rejection_status = 'rejected',
        rejection_reason = 'download time limit exceeded'
    where hash in (
        {hash_string}
    )
    """

    with conn.cursor() as cursor:
        cursor.execute(statement)
        rows_updated = cursor.rowcount
        conn.commit()

    conn.close()

    # needs to be properly connected to the correct environment
    for hash in hashes:
        utils.remove_media_item(hash)

    return rows_updated

# ------------------------------------------------------------------------------
# re-run metadata
# ------------------------------------------------------------------------------

def re_parse_hashes(hashes: list):
    """
    - reruns elements through the parsing pipeline; will return error if
        encountered, but will not otherwise update status
    - this function is environmentally locked, and not currently parameterizable

    :param hashes: list of hashes to rerun
    """
    # standard library imports
    import logging

    # third-party imports
    from dotenv import load_dotenv
    import polars as pl
    import yaml

    # local/custom imports
    from src.data_models import MediaDataFrame
    import src.utils as utils

    # ------------------------------------------------------------------------------
    # config
    # ------------------------------------------------------------------------------

    # load environment variables
    load_dotenv(override=True)

    # log config
    logger = logging.getLogger(__name__)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)

    # read in string special conditions
    with open('./config/string-special-conditions.yaml', 'r') as file:
        special_conditions = yaml.safe_load(file)

    # ------------------------------------------------------------------------------
    # title parse helper functions
    # ------------------------------------------------------------------------------

    def parse_media_items(media: MediaDataFrame) -> pl.DataFrame:
        """
        Parse the title of media items to extract relevant information
        :param media: MediaDataFrame contain all elements to be parsed
        :returns: DataFrame with parsed elements
        """
        # Create a copy of the input DataFrame
        parsed_media = media.df.clone()

        # Apply pre-processing replacements
        parsed_media = parsed_media.with_columns(
            cleaned_title=pl.col("original_title")
        )

        for old_str, new_str in special_conditions[
            'pre_processing_replacements']:
            parsed_media = parsed_media.with_columns(
                cleaned_title=pl.col("cleaned_title").str.replace(old_str,
                                                                  new_str)
            )

        # Extract common patterns using vectorized operations
        parsed_media = parsed_media.with_columns(
            resolution=pl.col("cleaned_title").map_elements(
                utils.extract_resolution, return_dtype=pl.Utf8),
            video_codec=pl.col("cleaned_title").map_elements(
                utils.extract_video_codec, return_dtype=pl.Utf8),
            audio_codec=pl.col("cleaned_title").map_elements(
                utils.extract_audio_codec, return_dtype=pl.Utf8),
            upload_type=pl.col("cleaned_title").map_elements(
                utils.extract_upload_type, return_dtype=pl.Utf8),
            uploader=pl.col("cleaned_title").map_elements(
                utils.extract_uploader, return_dtype=pl.Utf8)
        )

        # process based on media type
        movie_mask = parsed_media['media_type'] == "movie"
        if movie_mask.any():
            parsed_media = parsed_media.with_columns(
                release_year=pl.when(movie_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_year,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('release_year'))
            )

        tv_show_mask = parsed_media['media_type'] == "tv_show"
        if tv_show_mask.any():
            parsed_media = parsed_media.with_columns(
                season=pl.when(tv_show_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_season_from_episode,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('season')),
                episode=pl.when(tv_show_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_episode_from_episode,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('episode'))
            )

        tv_season_mask = parsed_media['media_type'] == "tv_season"
        if tv_season_mask.any():
            parsed_media = parsed_media.with_columns(
                season=pl.when(tv_season_mask)
                .then(pl.col('cleaned_title').map_elements(
                    utils.extract_season_from_season,
                    return_dtype=pl.Int32
                )).otherwise(pl.col('season'))
            )

        # extract media_title
        parsed_media = parsed_media.with_columns(
            media_title=pl.struct(["cleaned_title", "media_type"]).map_elements(
                lambda x: utils.extract_title(x["cleaned_title"],
                                              x["media_type"]),
                return_dtype=pl.Utf8
            )
        )

        # drop the cleaned title
        parsed_media.drop_in_place('cleaned_title')

        return parsed_media

    def validate_parsed_media(media: MediaDataFrame) -> pl.DataFrame:
        """
        validates media data based on the media type and update error status columns.
        :param media : MediaDataFrame containing parsed elements
        :returns verified_media: MediaDataFrame with verification check elements
            contained within
        """
        verified_media = media.df.clone()

        # Define mandatory fields for each media type
        mandatory_fields = {
            'all_media': ['media_title'],
            'movie': ['release_year'],
            'tv_show': ['season', 'episode'],
            'tv_season': ['season']
        }

        # verify mandatory fields for all media types
        for field in mandatory_fields['all_media']:
            verified_media = verified_media.with_columns(
                error_status=pl.when(pl.col(field).is_null())
                .then(pl.lit(True))
                .otherwise(pl.col('error_status')),
                error_condition=pl.when(pl.col(field).is_null())
                .then(
                    pl.when(pl.col('error_condition').is_null())
                    .then(pl.lit(f"{field} is null"))
                    .otherwise(pl.concat_str(
                        pl.col('error_condition'),
                        pl.lit(f"; {field} is null")
                    ))
                )
                .otherwise(pl.col('error_condition'))
            )

        # verify mandatory fields for specific media types
        movie_mask = verified_media['media_type'] == "movie"
        if movie_mask.any():
            for field in mandatory_fields['movie']:
                null_mask = pl.col(field).is_null()
                verified_media = verified_media.with_columns(
                    error_status=pl.when(movie_mask).then(
                        pl.when(null_mask)
                        .then(pl.lit(True))
                        .otherwise(pl.col("error_status")),
                    ).otherwise('error_status'),
                    error_condition=pl.when(movie_mask).then(
                        pl.when(null_mask)
                        .then(
                            pl.when(pl.col('error_condition').is_null())
                            .then(pl.lit(f"{field} is null"))
                            .otherwise(pl.concat_str(
                                pl.col('error_condition'),
                                pl.lit(f"; {field} is null")
                            ))
                        )
                        .otherwise(pl.col('error_condition'))
                    ).otherwise('error_condition')
                )

        tv_show_mask = verified_media['media_type'] == "tv_show"
        if tv_show_mask.any():
            for field in mandatory_fields['tv_show']:
                null_mask = pl.col(field).is_null()
                verified_media = verified_media.with_columns(
                    error_status=pl.when(tv_show_mask).then(
                        pl.when(null_mask)
                        .then(pl.lit(True))
                        .otherwise(pl.col("error_status"))
                    ).otherwise('error_status'),
                    error_condition=pl.when(tv_show_mask).then(
                        pl.when(null_mask)
                        .then(
                            pl.when(pl.col('error_condition').is_null())
                            .then(pl.lit(f"{field} is null"))
                            .otherwise(pl.concat_str(
                                pl.col('error_condition'),
                                pl.lit(f"; {field} is null")
                            ))
                        )
                        .otherwise(pl.col('error_condition'))
                    ).otherwise('error_condition')
                )

        tv_season_mask = verified_media['media_type'] == "tv_season"
        if tv_season_mask.any():
            for field in mandatory_fields['tv_season']:
                null_mask = pl.col(field).is_null()
                verified_media = verified_media.with_columns(
                    error_status=pl.when(tv_season_mask).then(
                        pl.when(null_mask)
                        .then(pl.lit(True))
                        .otherwise(pl.col("error_status"))
                    ).otherwise('error_status'),
                    error_condition=pl.when(tv_season_mask).then(
                        pl.when(null_mask)
                        .then(
                            pl.when(pl.col('error_condition').is_null())
                            .then(pl.lit(f"{field} is null"))
                            .otherwise(pl.concat_str(
                                pl.col('error_condition'),
                                pl.lit(f"; {field} is null")
                            ))
                        )
                        .otherwise(pl.col('error_condition'))
                    ).otherwise('error_condition')
                )

        return verified_media

    # ------------------------------------------------------------------------------
    # main execution
    # ------------------------------------------------------------------------------

    # read in existing data based on ingest_type
    media = utils.get_media_by_hash(hashes)

    # if no new data, return
    if media is None:
        return

    # iterate through all new movies, parse data from the title and add to new dataframe
    media.update(parse_media_items(media=media))

    # validate all essential fields are present
    media.update(validate_parsed_media(media))

    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        else:
            logging.info(f"parsed - {row['hash']}")

    # write parsed data back to the database
    utils.media_db_update(media=media)


def rerun_metadata(hashes: list):
    """
    - reruns all selected hashes through metadata collection
    - does commit errors to db, but does not otherwise update status
    - is locked to enviroment running from, not parameterizable like other
        functions here

    :param hashes: hash list of items to be reran
    """
    # standard library imports
    import json
    import logging
    import os
    import re
    import time

    # third-party imports
    from dotenv import load_dotenv
    import polars as pl
    import requests

    # local/custom imports
    import src.utils as utils
    from src.data_models import MediaDataFrame

    # ------------------------------------------------------------------------------
    # initialization and setup
    # ------------------------------------------------------------------------------

    # Load environment variables from .env file
    load_dotenv()

    # logger config
    logger = logging.getLogger(__name__)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)

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
            if media_item['release_year'] is not None:
                params = {
                    'query': media_item["media_title"],
                    'year': media_item['release_year'],
                    'api_key': tv_search_api_key
                }
            else:
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
            media_item['error_status'] = True
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

    # read in existing data
    media = utils.get_media_by_hash(hashes)

    # if no media to parse, return
    if media is None:
        return

    # search for media, and if not available reject
    updated_rows = []

    for idx, row in enumerate(media.df.iter_rows(named=True)):
        # pause at increments of 50 items to avoid rate limiting
        if (idx+1) % 50 == 0:
            logging.debug(f"completed metadata search batch {(idx+1)//50}")
            time.sleep(1)
        updated_row = media_search(row)
        updated_rows.append(updated_row)

    media = MediaDataFrame(updated_rows)

    # get additional media details
    updated_rows = []

    for idx, row in enumerate(media.df.iter_rows(named=True)):
        # pause at increments of 50 items to avoid rate limiting
        if (idx+1) % 50 == 0:
            logging.debug(f"completed metadata details batch {(idx+1)//50}")
            time.sleep(1)
        if not row['error_status'] and row['tmdb_id'] is not None :
            updated_row = collect_details(row)
        else:
            updated_row = row
        updated_rows.append(updated_row)

    media.update(pl.DataFrame(updated_rows))

    # get media rating metadata
    # updated_rows = []
    #
    # for idx, row in enumerate(media.df.iter_rows(named=True)):
    #     # pause 1 second between all items
    #     time.sleep(1)
    #     # pause at increments of 50 items to avoid rate limiting
    #     if (idx + 1) % 50 == 0:
    #         logging.debug(
    #             f"completed metadata rating batch {(idx + 1) // 50}")
    #         time.sleep(10)
    #     if not row['error_status']:
    #         updated_row = collect_ratings(row)
    #     else:
    #         updated_row = row
    #     updated_rows.append(updated_row)
    #
    # media.update(pl.DataFrame(updated_rows))

    # log succssefully collected items
    #for idx, row in enumerate(media.df.filter(~pl.col('error_status')).iter_rows(named=True)):
    #    logging.info(f"metadata collected - {row['hash']}")

    # write metadata back to the database
    utils.media_db_update(media=media)


def rerun_metadata_ratings(hashes: list, delay: int = 1):
    """
    - reruns all metadata ratings through metadata ratings API
    - does commit errors to db, but does not otherwise update status
    - is locked to environment running from, not parameterizable like other
        functions here

    :param hashes: hash list of items to be reran
    :param delay: number of seconds to wait between API calls
    """
    # standard library imports
    import json
    import logging
    import os
    import re
    import time

    # third-party imports
    from dotenv import load_dotenv
    import polars as pl
    import requests

    # local/custom imports
    import src.utils as utils
    from src.data_models import MediaDataFrame

    # ------------------------------------------------------------------------------
    # initialization and setup
    # ------------------------------------------------------------------------------

    # Load environment variables from .env file
    load_dotenv()

    # logger config
    logger = logging.getLogger(__name__)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)

    # load api env vars
    ratings_api_base_url = os.getenv('MOVIE_RATINGS_API_BASE_URL')
    ratings_api_key = os.getenv('MOVIE_RATINGS_API_KEY')

    def collect_ratings(media_item: dict) -> dict:
        """
        get ratings specific details for each media item, e.g. rt_score, metascore

        :param media_item: dict containing one for of media.df
        :return: dict of items with metadata added
        """
        # media_item = media.df.row(5, named=True)

        response = {}

        # Define the parameters for the OMDb API request
        if media_item['imdb_id'] is not None:
            params = {
                'i': media_item["imdb_id"],
                'apikey': ratings_api_key
            }
            logging.debug(f"collecting ratings for: {media_item['hash']} - imdb_id - {media_item['imdb_id']}")
        elif media_item['release_year'] is not None:
            params = {
                't': media_item["media_title"],
                'y': media_item["release_year"],
                'apikey': ratings_api_key
            }
            logging.debug(f"collecting ratings for: {media_item['hash']} - '{media_item['media_title']}' - {media_item['release_year']}")
        else:
            params = {
                't': media_item["media_title"],
                'apikey': ratings_api_key
            }
            logging.debug(f"collecting ratings for: {media_item['hash']} - '{media_item['media_title']}'")

        response = requests.get(ratings_api_base_url, params=params)
        status_code = response.status_code

        if response.status_code != 200:
            logging.error(f"media ratings API returned status code: {response.status_code}")
            media_item['error_status'] = True
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

    # -------------------------------------------------------------------------
    # primary function execution
    # -------------------------------------------------------------------------

    # read in existing data
    media = utils.get_media_by_hash(hashes)

    # if no media to parse, return
    if media is None:
        return

    # get media rating metadata
    updated_rows = []

    for idx, row in enumerate(media.df.iter_rows(named=True)):
        # pause between elements to avoid rate limiting
        time.sleep(delay)
        if not row['error_status'] and row['tmdb_id'] is not None:
            updated_row = collect_ratings(row)
        else:
            updated_row = row
        updated_rows.append(updated_row)

    media.update(pl.DataFrame(updated_rows))

    # log successfully collected items
    for idx, row in enumerate(media.df.filter(~pl.col('error_status')).iter_rows(named=True)):
       logging.info(f"metadata collected - {row['hash']}")

    # write metadata back to the database
    utils.media_db_update(media=media)


# ------------------------------------------------------------------------------
# make error handling function calls
# ------------------------------------------------------------------------------

hashes = [
"60b9bac376e07fac7e075160dbd768bc0ffee96c",
"5b3f8a858d897c9a7c3a13d55cbb6bf0d552d600",
"f094ed6d3b245399b76da720f160b6c968af6e36",
"a65f49c06238e41ec2cb5a2284695c73e938ab3e",
"b207d40a6b905396ba9e52401e6a032010ffc67f",
"f197d0d487949fa7121e4e2f2c7f1d05b40b8a95",
"2af776e332932779844e50622e40a5a2c316bc71",
"75e9467b458e0f3118e180f74709b98a73808c48",
"729b065f76bada78048877d23c526c16f818a3d8",
"504b926f57ad79959fc516595e946913a0f4f08f",
"f59f510ab8767eb285132e46578079142d9f9675",
"8ae3906ac9a0a57dc8c6a5ddf851ca467ba887b4",
"f337b489999cf6a80b5a3d3872b8f172d23cca08",
"bf08a5907dc24c7972eb258e072c0abbdff9194d",
"a7f8923075bffd1d790e3af40a2da1b58e62843d",
"9b48b31db8ba890fe03dda7e3741d6cbbfba7cf9",
"82a38d4772b66aa2c38bbe32471b523a930a8d8e",
"bc825cdb3280854e018491b2b23a6b43b120fd8c",
"fc38eba2fb9ba65d1174ba8da91ade637f72649e",
"67c9627646046a89a4a895923f0927a76a2a38bb",
"77bc761b44337e9b4936c0f9af2a5d9eda487cae",
"c7b829bc4de900af887b758b147ed0643691f640",
"f2cd76118fe04e4ec8c99b79cf89d35e38ba7a57",
"75c008d85edab8d9cfec97beff3daee049adc81f",
"4262d57d04cb667a6af5615d00028685606f8499",
"63db7548d64f72f9f32f2b9f9131aedf9464ec49",
"f2b9f2c232a61091d860121113fb12179a0f6455",
"366c24adc7f2030e33d4a3d8fdc866ac3ac195b6",
"da52cfdf01cf659b6c14ace2a60e39dc5db64a33",
"4a8e37936aebf84c6bf507f2b3c0e71b0103289c",
"840fc9a96438eb723ff964cedae6b2aef10d16d3",
"88f56b97893b1c1e30195c3514e1c46a00e2e252",
"3e6e72633882289c378731be48bd67ca9c431da5",
"efe7a69ee1b3c253b38e4cab3b3f315aa24837a6",
"2e3ee861a7d92dd6a7439abcec034473bf01a185",
"599a0e5ff4b15a0adb20689d6ec6ae1569f8c971",
"25709713774db1768723d153a0e91b0cada166eb",
"913ef62e5ae0aa2f6cdb98f114195c8ba0e00756",
"43d542c935353aee21aa2290eaff569b8e56a772",
"e5e7aed4810ad23f665c39722231b3b3612773ef",
"6f78e911ab0f005abb39633494c1644b8c4c4c9f",
"002662b394a5cd6263e188fbd2f23fb42f3a645d",
"49958fa179239a4ef12573b1695fb8df271a03ec",
"3cc7914d746fad28a57b11a4ddde7b4dade17168",
"1c84643c7954439b7b7e931bc1220203906fdac5",
"48912c2f0c7ab72323cc43318fad75350b716ed1",
"4ecd5b6f205c16538ee04ddb9f6d7bacc9a198ee",
"9644bcf317022c056583789bbf9c7ad56eac9eb5",
"5301b9cf209eeea9c7dcfd4bae816cde05c46250",
"aa78f37f5da8b78f69425d0b6bec20d7a013a5cf",
"aeedfa674cd5526ac6ca32f993cfc54e33ca54be",
"b1e594b3c66250b6867f821072b28e9a82cee09d",
"e4c5d3be523e9e2f291185c59474826e4991c6dd",
"e2aa166e6d8c3db542aee60931a27ff74e48391f",
"38399711354df9939bceaf398cc6a51e0e897a87",
"70ed2432b4effc5b1208af8ab6090f4b89eb8d06",
"f817fb6620919f8f90246244c670365d66094f6b",
"efea7c19577eceee93928a694e05169f97be2c3f",
"76b8e457f2b6c8bd8b9c41fd01f652ff75e8d62b",
"623afbdcfa0004c81a32f4ff9b11b4291aece283",
"d18e808581bd39746c182452f439ff9ef58f676f",
"ac18c4b3d63402f2c572e623f715c05604f33a7c",
"cba0cdcfd86b61d466b25b03db13008fd49c8e97",
"a36b1bc8698871d877ab6fe2d276be293323fe2f",
"7c2b6feb21d32bc99a2ca2f4ed25985d81c5e5cb",
"914d5caf0c4e392c857e2db78f169bcff61440a8",
"ec6351f15d79b51cee9c20868e41b0285458afaf",
"dff4201d238a3e78abd9fef6f604488bf297a166",
"6c059fcbab9924f4ddcc131bdd63e32fdbfda8d2",
"87a0c42e5ec626b7f5643b4c0e56b3dc23dd1bc0",
"8f9ac8e595a98d0e85d5376e95568a8661b9b34c",
"2c1f40c88b902959b0f4ddccdf94a468db1e3b3c",
"85c73011b348c81fa3f7fe7fff2289b7dd5fc84e",
"0968de4917ca45473be4fbfc195c18af3c2be673",
"0fe6fc439776ab775eb2036e9e0fdb2be5e96b68",
"cbc7301641edfde9063775b28b9f3bba42b5723e",
"066cca2e16f260819b267582dc24be0d8553c1cf",
"8c717510a50a0fe2a92e8cbcce0827b120996eab",
"07df0c3f5102cd4173611077fc344a2ca952a162"
]

reingest_items(hashes, 'stg')

# ------------------------------------------------------------------------------
# end of error_handling.py
# ------------------------------------------------------------------------------
