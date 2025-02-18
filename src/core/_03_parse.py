# standard library imports
import logging

# third-party imports
from dotenv import load_dotenv
import pandas as pd

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------

# load environment variables
load_dotenv()

# logger config
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# title parse helper functions
# ------------------------------------------------------------------------------

def parse_item(media_item: pd.Series, media_type: str) -> pd.Series:
    """
    Parse the title of a new item to extract relevant information
    :param media_item: element from rss feed data frame
    :param media_type: either "movie" or "tv_show"
    :raises ValueError: if mandatory fields are missing
    :return: new item series to be inserted into master data frame
    """
    raw_title = media_item['raw_title']

    # Define mandatory fields for each media type
    mandatory_fields = {
        'movie': ['movie_title', 'release_year'],
        'tv_show': ['tv_show_name', 'season', 'episode'],
        'tv_season': ['tv_show_name', 'season']
    }

    # search for patterns in both item types
    media_item['resolution']  = utils.extract_resolution(raw_title)
    media_item['video_codec'] = utils.extract_video_codec(raw_title)
    media_item['audio_codec'] = utils.extract_audio_codec(raw_title)
    media_item['upload_type'] = utils.extract_upload_type(raw_title)
    media_item['uploader']    = utils.extract_uploader(raw_title)

    # search for movie only patterns
    if media_type == 'movie':
       media_item['movie_title']  = utils.extract_title(raw_title, media_type)
       logging.debug(f"raw_title: {raw_title} has tv_show_name {media_item['movie_title']}")
       media_item['release_year'] = utils.extract_year(raw_title)
    # search for tv show only patterns
    elif media_type == 'tv_show':
        media_item['tv_show_name'] = utils.extract_title(raw_title, media_type)
        logging.debug(f"raw_title: {raw_title} has tv_show_name {media_item['tv_show_name']}")
        media_item['season'] = utils.extract_season_from_episode(raw_title)
        media_item['episode'] = utils.extract_episode_from_episode(raw_title)
    # search for tv season only pattens
    elif media_type == 'tv_season':
        media_item['tv_show_name'] = utils.extract_title(raw_title, media_type)
        logging.debug(f"raw_title: {raw_title} has tv_show_name {media_item['tv_show_name']}")
        media_item['season'] = utils.extract_season_from_season(raw_title)
    else:
        raise ValueError("Invalid item type. Must be 'movie' or 'tv_show'")

    # Validate mandatory fields for the specific media type
    is_valid, missing = utils.validate_series(
        entry_series=media_item,
        mandatory_fields=mandatory_fields[media_type]
    )
    if not is_valid:
        raise ValueError(
            f"Missing or empty mandatory fields {missing} for item: {raw_title}")

    return media_item


# ------------------------------------------------------------------------------
# full title parse pipeline
# ------------------------------------------------------------------------------

def parse_media(media_type: str):
    """
    Full ingest pipeline for either movies or tv shows
    :param media_type: either "movie" or "tv_show"
    :return:
    """
    #media_type='movie'
    #media_type='tv_show'
    #media_type='tv_season'

    # read in existing data based on ingest_type
    media = utils.get_media_from_db(
        media_type=media_type,
        status='ingested'
    )

    media_parsed = pd.DataFrame()

    # iterate through all new movies, parse data from the title and add to new dataframe
    if len(media) > 0:
        media_parsed = media.copy().iloc[0:0]

        for index in media.index:
            try:
                # will raise error if mandatory field not populated
                parsed_item = parse_item(
                    media_item=media.loc[index].copy(),
                    media_type=media_type
                )
                media_parsed = pd.concat([media_parsed, parsed_item.to_frame().T])
                logging.info(f"parsed: {media.loc[index, 'raw_title']}")
            except Exception as e:
                logging.error(f"parse_media_items error: {e}")
                logging.error(f"failed to parse: {media.loc[index, 'raw_title']}")

    if len(media_parsed) > 0:
        # write parsed data back to the database
        utils.update_db_media_table(
            media_type=media_type,
            media_old=media,
            media_new=media_parsed
        )

        # update status of successfully parsed items
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=media_parsed.index.tolist(),
            new_status='parsed'
        )

# ------------------------------------------------------------------------------
# end of _03_parse.py
# ------------------------------------------------------------------------------