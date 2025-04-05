# standard library imports
import logging
import os

# third-party imports
from dotenv import load_dotenv
import feedparser
import polars as pl
from feedparser import FeedParserDict

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

def rss_feed_ingest(
    rss_url: str,
    rss_source: str
) -> list:
    """
    ping rss feed and store the input
    :param rss_url: url of rss feed
    :param rss_source: source of rss feed as a base url
    :return: list of rss entries
    """
    # call rss feed
    feed = feedparser.parse(rss_url)

    # print terminal message
    logging.debug("ingesting from: " + str(feed.channel.title))
    logging.debug(f"feed: {str(feed)[:60]}...")

    # extract entires
    entries = feed['entries']

    # append rss_source
    for entry in entries:
        entry['rss_source'] = rss_source

    return feed['entries']

def format_entries(entry: FeedParserDict) -> dict:
    """
    converts raw rss entries into dicts suitable for ingestion into a MediaDataFrame
    :param entry: FeedParserDict
    :return: entry dict suitable for MediaDataFrame insertion
    """
    formatted_entry = {}

    # format entries based off of rss source
    if entry['rss_source'] == "yts.mx":
        formatted_entry['hash'] = utils.extract_hash_from_direct_download_url(
            entry['links'][1]['href'])
        formatted_entry['media_type'] = "movie"
        formatted_entry['original_title'] = entry['title']
        formatted_entry['original_link'] = entry['links'][1]['href']
    elif entry['rss_source'] == "episodefeed.com":
        formatted_entry['hash'] = utils.extract_hash_from_magnet_link(
            entry['link'])
        formatted_entry['media_type'] = utils.classify_media_type(
            entry['title'])
        formatted_entry['media_title'] = entry['tv_show_name']
        formatted_entry['original_title'] = entry['title']
        formatted_entry['original_link'] = entry['link']

    return formatted_entry


# ------------------------------------------------------------------------------
# full ingest for either element type
# ------------------------------------------------------------------------------

def rss_ingest():
    """
    full ingest pipeline for either movies or tv shows
    """
    # retrieve rss feed based on ingest_type
    rss_sources = os.getenv('RSS_SOURCES').split(',')
    rss_urls = os.getenv('RSS_URLS').split(',')

    # confirm sources and urls or equal length, and if not return
    if len(rss_sources) != len(rss_urls):
        logging.error("env var rss_sources does not match length of rss_urls")
        return

    # retrieve entries from all rss feeds
    all_entries = []
    for i in range(len(rss_sources)):
        new_entries = rss_feed_ingest(
            rss_url=rss_urls[i],
            rss_source=rss_sources[i]
        )
        all_entries.extend(new_entries)

    # format each entry for conversion to MediaDataFrame
    formatted_entries = [format_entries(entry=entry) for entry in all_entries]

    # convert to MediaDataFrame object
    # conversion to MediaDataFrame object will handle element verification
    media = MediaDataFrame(formatted_entries)

    # determine which feed entries are new entries
    #new_hashes = feed_items['hash'].to_list()
    new_hashes = utils.compare_hashes_to_db(
        hashes=media.df['hash'].to_list()
    )

    if len(new_hashes) > 0:
        media.update(media.df.filter(pl.col('hash').is_in(new_hashes)))

        # write new items to the database
        utils.insert_items_to_db(media=media)

        # update status of ingested items
        utils.update_db_pipeline_status_by_hash(
            hashes=new_hashes,
            new_pipeline_status='ingested'
        )

        for row in media.df.iter_rows(named=True):
            logging.info(f"ingested: {row['original_title']}")

# ------------------------------------------------------------------------------
# end of _01_rss_ingest.py
# ---------------------------------------------------------------------------