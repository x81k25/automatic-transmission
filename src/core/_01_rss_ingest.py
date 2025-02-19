# standard library imports
import json
import logging
import os

# third-party imports
from dotenv import load_dotenv
import feedparser
import pandas as pd

# local/custom imports
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

# load env vars
load_dotenv()

# logging config
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# rss ingest helper functions
# ------------------------------------------------------------------------------

def rss_feed_ingest(rss_url: str) -> feedparser.FeedParserDict:
    """
    ping rss feed and store the input
    :param rss_url: url of rss feed
    :return:
    """
    # ping rss feed
    feed = feedparser.parse(rss_url)

    # print terminal message
    logging.info("ingesting from: " + str(feed.channel.title))
    logging.debug(f"feed: {str(feed)[:60]}...")

    return feed


def rss_entries_to_dataframe(
    feed: feedparser.FeedParserDict,
    media_type: str
) -> pd.DataFrame:
    """
    Convert RSS feed entries into a pandas DataFrame
    :param feed: extracted rss feed
    :param media_type: type of feed, either "movie" or "tv_show"
    :return: DataFrame containing the RSS feed entries
    """
    logging.debug(f"Feed type and keys: {type(feed)}, Keys: {feed.keys() if isinstance(feed, dict) else 'Not a dict'}")
    logging.debug(f"Number of entries: {len(feed['entries'])}, First entry keys: {feed['entries'][0].keys() if feed['entries'] else 'No entries'}")

    # Extract the entries
    entries = feed['entries']

    # Extract relevant fields from each entry
    extracted_data = []

    if media_type == 'movie':
        for entry in entries:
            extracted_dict= {
                'hash': utils.extract_hash_from_direct_download_url(entry['links'][1]['href']),
                'raw_title': entry['title'],
                'torrent_source': entry['links'][1]['href'],
            }
            utils.validate_dict(extracted_dict)
            extracted_data.append(extracted_dict)
    elif media_type == 'tv_show':
        for entry in entries:
            extracted_dict = {
                'hash': utils.extract_hash_from_magnet_link(entry['link']),
                'raw_title': entry['title'],
                'torrent_source': entry['link']
            }
            utils.validate_dict(extracted_dict)
            if utils.classify_media_type(extracted_dict['raw_title']) == 'tv_show':
                extracted_data.append(extracted_dict)
    elif media_type == 'tv_season':
        for entry in entries:
            extracted_dict = {
                'hash': utils.extract_hash_from_magnet_link(entry['link']),
                'raw_title': entry['title'],
                'torrent_source': entry['link']
            }
            utils.validate_dict(extracted_dict)
            if utils.classify_media_type(extracted_dict['raw_title']) == 'tv_season':
                extracted_data.append(extracted_dict)
    else:
        raise ValueError("Invalid feed type. Must be 'movie' or 'tv_show'")

    # Convert extracted data to DataFrame
    feed_items = pd.DataFrame(extracted_data)
    # set hash as index
    feed_items.set_index('hash', inplace=True)

    return feed_items


# ------------------------------------------------------------------------------
# full ingest for either element type
# ------------------------------------------------------------------------------

def rss_ingest(media_type: str):
    """
    Full ingest pipeline for either movies or tv shows

    :param media_type: either "movie" or "tv_show"
    """
    #media_type='movie'
    #media_type='tv_show'
    # retrieve rss feed based on ingest_type
    rss_url = None
    if media_type == 'movie':
        rss_url = os.getenv('MOVIE_RSS_URL')
    # the tv show feed may occasionally contain tv seasons
    elif media_type == 'tv_show' or media_type == 'tv_season':
        rss_url = os.getenv('TV_SHOW_RSS_URL')

    feed = rss_feed_ingest(rss_url)

    # convert feed to data frame
    feed_items = rss_entries_to_dataframe(
        feed=feed,
        media_type=media_type
    )

    # determine which feed entries are new entries
    feed_hashes = feed_items.index.tolist()

    new_hashes = utils.compare_hashes_to_db(
        media_type=media_type,
        hashes=feed_hashes
    )

    if len(new_hashes) > 0:
        new_items = feed_items.loc[new_hashes]

        # write new items to the database
        utils.insert_items_to_db(
            media_type=media_type,
            media=new_items
        )

        # update status of ingested items
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=new_hashes,
            new_status='ingested'
        )

        for index in new_items.index:
            logging.info(f"ingested: {new_items.loc[index, 'raw_title']}")

# ------------------------------------------------------------------------------
# end of _01_rss_ingest.py
# ---------------------------------------------------------------------------