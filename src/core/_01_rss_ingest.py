# standard library imports
import logging
import os

# third-party imports
import feedparser
import polars as pl
from feedparser import FeedParserDict

# local/custom imports
import src.utils as utils
from src.data_models import MediaSchema

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
    :return: list of rss entries (empty list if feed fails)
    """
    try:
        # call rss feed
        feed = feedparser.parse(rss_url)

        # check if feed parsing failed (bozo bit indicates malformed/failed feed)
        if hasattr(feed, 'bozo') and feed.bozo:
            if hasattr(feed, 'bozo_exception'):
                logging.error(f"RSS feed parsing failed for {rss_source} ({rss_url}): {feed.bozo_exception}")
            else:
                logging.error(f"RSS feed parsing failed for {rss_source} ({rss_url}): Unknown error")

        # print terminal message with error handling for missing channel.title
        try:
            feed_title = feed.channel.title
        except AttributeError:
            feed_title = rss_source
            logging.warning(f"Could not access channel.title for {rss_source}, using source name instead")

        logging.debug("ingesting from: " + str(feed_title))

        # extract entries
        entries = feed['entries']

        # check if feed returned no entries and log appropriately
        if len(entries) == 0:
            logging.warning(f"No entries found in RSS feed for {rss_source} ({rss_url})")

        # append rss_source
        for entry in entries:
            entry['rss_source'] = rss_source

        return entries

    except Exception as e:
        logging.error(f"Failed to ingest RSS feed from {rss_source} ({rss_url}): {e}")
        return []


def format_entries(entry: FeedParserDict) -> dict:
    """
    converts raw rss entries into dicts suitable for ingestion into a MediaDataFrame
    :param entry: FeedParserDict
    :return: entry dict suitable for MediaDataFrame insertion (None if formatting fails)
    """
    formatted_entry = {}

    try:
        # format entries based off of rss source
        if entry['rss_source'] in ["yts.mx", "yts.lt"]:
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
        else:
            logging.warning(f"Unknown RSS source: {entry.get('rss_source', 'N/A')}")
            return None

    except Exception as e:
        logging.error(f"Failed to format entry from {entry.get('rss_source', 'unknown')}: {e}")
        return None

    return formatted_entry


def log_status(media: pl.DataFrame) -> None:
    """
    logs acceptance/rejection of the media filtration process

    :param media: DataFrame containing process values to be printed
    :return: None
    """
    for row in media.iter_rows(named=True):
        logging.info(f"ingested - {row['hash']}")

# ------------------------------------------------------------------------------
# full ingest for either element type
# ------------------------------------------------------------------------------

def rss_ingest():
    """
    full ingest pipeline for either movies or tv shows
    """
    # retrieve rss feed based on ingest_type
    rss_sources = os.getenv('AT_RSS_SOURCES').split(',')
    rss_urls = os.getenv('AT_RSS_URLS').split(',')

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

    # filter out None entries (failed formatting)
    formatted_entries = [entry for entry in formatted_entries if entry is not None]

    # remove duplicates by hash, keeping first occurrence
    seen_hashes = set()
    unique_entries = []
    for entry in formatted_entries:
        if 'hash' in entry and entry['hash'] not in seen_hashes:
            seen_hashes.add(entry['hash'])
            unique_entries.append(entry)

    # early return if no entries
    if not unique_entries:
        return

    # convert to DataFrame
    media = pl.DataFrame(unique_entries)

    # determine which feed entries are new entries
    new_hashes = utils.compare_hashes_to_db(hashes=media['hash'].to_list())
    media = media.filter(pl.col('hash').is_in(new_hashes))

    if media.height > 0:
        # validate and write new items to the database
        media = MediaSchema.validate(media)
        utils.insert_items_to_db(media=media)
        log_status(media)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    rss_ingest()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _01_rss_ingest.py
# ---------------------------------------------------------------------------