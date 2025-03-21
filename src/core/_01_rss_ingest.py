# standard library imports
import logging
import os

# third-party imports
from dotenv import load_dotenv
import feedparser
import polars as pl

# local/custom imports
import src.utils as utils
from src.data_models import MediaDataFrame

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
    :return: FeedParser dict element
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
) -> MediaDataFrame:
    """
    convert RSS feed entries into a structured DataFrame
    :param feed: extracted rss feed
    :param media_type: type of feed, either "movie", "tv_show", or "tv_season"
    :return: Appropriate DataFrame object based on media_type
    """
    logging.debug(
        f"Feed type and keys: {type(feed)}, Keys: {feed.keys() if isinstance(feed, dict) else 'Not a dict'}")
    logging.debug(
        f"Number of entries: {len(feed['entries'])}, First entry keys: {feed['entries'][0].keys() if feed['entries'] else 'No entries'}")

    # Extract the entries
    entries = feed['entries']

    # Extract relevant fields from each entry
    extracted_data = []

    if media_type == 'movie':
        for entry in entries:
            extracted_dict = {
                'hash': utils.extract_hash_from_direct_download_url(
                    entry['links'][1]['href']),
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
            if utils.classify_media_type(
                extracted_dict['raw_title']) == 'tv_show':
                extracted_data.append(extracted_dict)

    elif media_type == 'tv_season':
        for entry in entries:
            extracted_dict = {
                'hash': utils.extract_hash_from_magnet_link(entry['link']),
                'raw_title': entry['title'],
                'torrent_source': entry['link']
            }
            utils.validate_dict(extracted_dict)
            if utils.classify_media_type(
                extracted_dict['raw_title']) == 'tv_season':
                extracted_data.append(extracted_dict)

    else:
        raise ValueError(
            "Invalid feed type. Must be 'movie', 'tv_show', or 'tv_season'")

    # return MediaDataFrame instance
    return utils.MediaDataFrame(extracted_data)


# ------------------------------------------------------------------------------
# full ingest for either element type
# ------------------------------------------------------------------------------

def rss_ingest(media_type: str):
    """
    full ingest pipeline for either movies or tv shows
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

    # convert feed to MediaDataFrame
    feed_media = rss_entries_to_dataframe(
        feed=feed,
        media_type=media_type
    )

    # determine which feed entries are new entries
    #new_hashes = feed_items['hash'].to_list()
    new_hashes = utils.compare_hashes_to_db(
        media_type=media_type,
        hashes=feed_media.df['hash'].to_list()
    )

    if len(new_hashes) > 0:
        feed_media._df = feed_media.df.filter(pl.col('hash').is_in(new_hashes))

        # write new items to the database
        utils.insert_items_to_db(
            media_type=media_type,
            media=feed_media
        )

        # update status of ingested items
        utils.update_db_status_by_hash(
            media_type=media_type,
            hashes=new_hashes,
            new_status='ingested'
        )

        for row in feed_media.df.iter_rows(named=True):
            logging.info(f"ingested: {row['raw_title']}")

# ------------------------------------------------------------------------------
# end of _01_rss_ingest.py
# ---------------------------------------------------------------------------