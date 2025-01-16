from dotenv import load_dotenv
import feedparser
import os
import pandas as pd
import src.utils as utils

# ------------------------------------------------------------------------------
# load environment variables
# ------------------------------------------------------------------------------

load_dotenv()

# ------------------------------------------------------------------------------
# rss ingest helper functions
# ------------------------------------------------------------------------------

def rss_feed_ingest(rss_url):
    """
    ping rss feed and store the input
    :param rss_url: url of rss feed
    :return:
    """
    # ping rss feed
    feed = feedparser.parse(rss_url)

    # print terminal message
    utils.log("ingesting from: " + str(feed.channel.title))

    return feed


def rss_entries_to_dataframe(feed, media_type):
    """
    Convert RSS feed entries into a pandas DataFrame
    :param feed: extract rss feed
    :param media_type: type of feed, either "movie" or "tv_show"
    :return: DataFrame containing the RSS feed entries
    """
    # Extract the entries
    entries = feed['entries']

    # Extract relevant fields from each entry
    extracted_data = []

    if media_type == 'movie':
        for entry in entries:
            extracted_data.append({
                'hash': entry.links[1].href.split('/')[-1],
                'raw_title': entry.title,
                'torrent_source': entry.links[1].href,
                'published_timestamp': entry.published,
            })
    elif media_type == 'tv_show':
        for entry in entries:
            extracted_data.append({
                'hash': entry.get('tv_info_hash'),
                'tv_show_name': entry.get('tv_show_name'),
                'torrent_source': entry.get('link'),
                'published_timestamp': entry.get('published'),
                'summary': entry.get('summary'),
                'raw_title': entry.get('title')
            })
    else:
        raise ValueError("Invalid feed type. Must be 'movie' or 'tv_show'")

    # convert all the hash values to lower case
    for entry in extracted_data:
        entry['hash'] = entry['hash'].lower()

    # special exceptions for known issues
    for entry in extracted_data:
        if media_type == 'tv_show':
            if entry['tv_show_name'] == '60 Minutes (US)':
                entry['tv_show_name'] = '60 Minutes'

    # Convert extracted data to DataFrame
    feed_items = pd.DataFrame(extracted_data)
    # set hash as index
    feed_items.set_index('hash', inplace=True)

    return feed_items

# ------------------------------------------------------------------------------
# full ingest for either element type
# ------------------------------------------------------------------------------

def rss_ingest(media_type):
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
    elif media_type == 'tv_show':
        rss_url = os.getenv('TV_SHOW_RSS_URL')

    feed = rss_feed_ingest(rss_url)

    # convert feed to data frame
    feed_items = rss_entries_to_dataframe(
        feed=feed,
        media_type=media_type
    )

    # instantiate db engine
    engine = utils.sqlf.create_db_engine()

    # determine which feed entries are new entries
    feed_hashes = feed_items.index.tolist()

    new_hashes = utils.compare_hashes_to_db(
        media_type=media_type,
        hashes=feed_hashes,
        engine=engine
    )

    if len(new_hashes) > 0:
        new_items = feed_items.loc[new_hashes]

        # write new items to the database
        utils.insert_items_to_db(
            media_type=media_type,
            media=new_items,
            engine=engine
        )

        # update status of ingested items
        utils.update_db_status_by_hash(
            engine=engine,
            media_type=media_type,
            hashes=new_hashes,
            new_status='ingested'
        )

        for index in new_items.index:
            utils.log(f"ingested: {new_items.loc[index, 'raw_title']}")

# ------------------------------------------------------------------------------
# end of _01_rss_ingest.py
# ------------------------------------------------------------------------------