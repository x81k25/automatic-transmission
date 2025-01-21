from dotenv import load_dotenv
import json
import os
import pandas as pd
import re
import requests
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# omdb environment variables
omdb_base_url = os.getenv('OMDB_BASE_URL')
api_key = os.getenv('OMDB_API_KEY')

# ------------------------------------------------------------------------------
# collector helper functions
# ------------------------------------------------------------------------------

def classify_media(raw_title):
    """
    Classify media as either movie, tv_show, or tv_season based on the title

    :param raw_title: raw title string of media item
    :return: string indicating media type
    """

    # Core patterns
    # TV show must have SxxExx pattern (case insensitive)
    tv_pattern = r'[Ss]\d{2}[Ee]\d{2}'

    # TV season can have either "season X" or "sXX" pattern (case insensitive)
    # Looks for season/s followed by 1-2 digits, not followed by episode pattern
    season_pattern = r'(?:[Ss]eason\s+\d{1,2}|[Ss]\d{1,2}(?![Ee]\d{2}))'

    # Movie pattern now accounts for years with or without parentheses
    # Matches 19xx or 20xx with similar delimiters on both sides
    movie_pattern = r'(?:^|\W)((?:19|20)\d{2})(?:$|\W)'

    if re.search(tv_pattern, raw_title):
        return "tv_show"
    elif re.search(season_pattern, raw_title):
        return "tv_season"
    elif re.search(movie_pattern, raw_title):
        return "movie"

    raise ValueError(f"Unable to classify media title: '{raw_title}'")


def extract_name(raw_title, media_type):
    """
    Extract and format the movie name for OMDb API queries
    :param raw_title: raw_title string of media item
    :param media_type: type of collection, either "movie" or "tv_show"
    :return: string of movie name formatted for OMDb API
    """
    if media_type == 'movie':
        # Remove quality info and encoding info that comes after the year
        # Match year pattern (with or without parentheses) and everything after
        cleaned = re.split(r'(?:^|\s)\(?(?:19|20)\d{2}\)?[^\w]*.*$', raw_title)[0]

        # Replace periods with spaces
        cleaned = cleaned.replace('.', ' ')

        # Special case: Add apostrophe for "Its" -> "It's"
        cleaned = cleaned.replace('Its ', "It's ")

        # Remove any special characters except spaces, apostrophes, and alphanumerics
        cleaned = re.sub(r'[^a-zA-Z0-9\' ]', '', cleaned)

        # Clean up any extra whitespace
        cleaned = ' '.join(cleaned.split())
    elif media_type == 'tv_show':
        # Remove quality info and encoding info
        cleaned = re.sub(r'\d{3,4}p.*$', '', raw_title)

        # Get everything before the SxxExx pattern
        cleaned = re.split(r'[Ss]\d{2}[Ee]\d{2}', cleaned)[0]

        # Replace periods with spaces
        cleaned = cleaned.replace('.', ' ')

        # Special case: Add apostrophe for "Its" -> "It's"
        cleaned = cleaned.replace('Its ', "It's ")

        # Remove any special characters except spaces, apostrophes, and alphanumerics
        cleaned = re.sub(r'[^a-zA-Z0-9\' ]', '', cleaned)

        # Clean up any extra whitespace
        cleaned = ' '.join(cleaned.split())
    elif media_type == 'tv_season':
        # Remove quality info and encoding info that comes after season pattern
        # Matches either "Season XX" or "SXX" pattern (case insensitive)
        cleaned = \
            re.split(r'(?:[Ss]eason\s+\d{1,2}|[Ss]\d{1,2})[^\w]*.*$',
                     raw_title)[0]

        # Remove any year in parentheses that's adjacent to show name
        cleaned = re.sub(r'\s*\([12][90]\d{2}\)\s*', ' ', cleaned)

        # Replace periods and other special characters with spaces
        cleaned = cleaned.replace('.', ' ')

        # Special case: Add apostrophe for "Its" -> "It's"
        cleaned = cleaned.replace('Its ', "It's ")

        # Remove any special characters except spaces, apostrophes, and alphanumerics
        cleaned = re.sub(r'[^a-zA-Z0-9\' ]', '', cleaned)

        # Clean up any extra whitespace
        cleaned = ' '.join(cleaned.split())
    else:
        cleaned =None

    return cleaned


def verify_omdb_retrievable(cleaned_title):
    """
    determine if item is OMDB retrievable to confirm if the title parsing
    was conducted successfully; no metadata will actually be retrieved at this
    point; metadata retrieval will be conducted in _04_metadata_collection.py
    :param cleaned_title: cleaned string of name of media item
    :return:
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


def collected_torrents_to_dataframe(media_items, media_type):
    """
    format collected media items into a pandas dataframe for database ingestion
    :param media_items: dictionary of media items
    :param media_type: type of media to collect, either 'movie' or 'tv_show'
    :return:
    """

    df = pd.DataFrame.from_dict(media_items, orient='index')

    if media_type == 'movie':
        df = df.rename(columns={
            'name': 'raw_title',
            'torrent_source': 'torrent_source'
        })
        df = df[['raw_title', 'torrent_source']]
    elif media_type == 'tv_show' or media_type == 'tv_season':
        df = df.rename(columns={
            'name': 'raw_title',
            'cleaned_title': 'tv_show_name',
            'torrent_source': 'torrent_source'
        })
        df = df[['raw_title', 'tv_show_name', 'torrent_source']]

    return df

# ------------------------------------------------------------------------------
# collect main function
# ------------------------------------------------------------------------------

def collect_media(media_type):
    """
    collect ad hoc items added to transmission not from rss feeds and insert
    into automatic-transmission pipeline

    :param media_type: type of media to collect, either 'movie' or 'tv_show'
    """
    #media_type = 'movie'
    #media_type = 'tv_show'
    #media_type = 'tv_season'

    # get torrents currently in transmission
    # if no torrents in transmission end function
    current_media = utils.return_current_torrents()

    if current_media is None:
        return

    # determine media type and keep only the desired type
    to_remove = []

    for hash_id, item_data in current_media.items():
        try:
            item_media_type = classify_media(item_data['name'])
            if item_media_type != media_type:
                to_remove.append(hash_id)
        except Exception as e:
            to_remove.append(hash_id)
            utils.log(f"failed to classify media_type: {current_media[hash_id]['name']}")
            utils.log(f"collect_media error: {e}")

    # if no items match calssifaction end function
    if len(current_media) == 0:
        return

    for hash_id in to_remove:
        del current_media[hash_id]

    # extract clean item name from raw_title
    for hash_id, item_data in current_media.items():
        current_media[hash_id]['cleaned_title'] = extract_name(item_data['name'], media_type)

    # determine if item is omdb retrievable, if not raise error and remove from items
    to_remove = []

    for hash_id, item_data in current_media.items():
        try:
            verify_omdb_retrievable(cleaned_title=item_data['cleaned_title'])
        except Exception as e:
            to_remove.append(hash_id)
            utils.log(f"failed to retrieve OMDB metadata: {current_media[hash_id]['name']}")
            utils.log(f"collect_media error: {e}")

    for hash_id in to_remove:
        del current_media[hash_id]

    # if no items are OMDB retrievable end function
    if len(current_media) == 0:
        return

    # determine which items are new
    new_hashes = utils.compare_hashes_to_db(
        media_type=media_type,
        hashes=list(current_media.keys())
    )

    # determine which items were previously reject
    rejected_hashes = utils.return_rejected_hashes(
        media_type=media_type,
        hashes=list(current_media.keys())
    )

    # convert to dataframe for db ingestion
    collected_media_items = collected_torrents_to_dataframe(
        media_items=current_media,
        media_type=media_type
    )

    # create separate data frames for new items
    new_items = collected_media_items[collected_media_items.index.isin(new_hashes)]

    # add new items to db and set statuses
    if len(new_items) > 0:

        # insert new items to db
        utils.insert_items_to_db(
            media_type=media_type,
            media=new_items
        )

        # update status
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=list(new_items.index),
            new_status='ingested'
        )

        # update rejection status
        utils.update_rejection_status_by_hash(
            media_type=media_type,
            hashes=list(new_items.index),
            new_status='override'
        )

        # print log
        for index in new_items.index:
            utils.log(f"collected: {new_items.loc[index, 'raw_title']}")

    # update statuses of items that have been previously rejected
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

        # print log
        for hash in rejected_hashes:
            raw_title = collected_media_items.loc[collected_media_items['hash'] == hash, 'name'].iloc[0]
            utils.log(f"collected: {raw_title}")

# ------------------------------------------------------------------------------
# end of _02_collect.py
# ------------------------------------------------------------------------------